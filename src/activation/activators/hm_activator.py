"""High Mobility OEM Activator - Handles activation and processing for HM vehicles."""

import asyncio
import logging
from datetime import datetime

import aiohttp
import pandas as pd
from sqlalchemy import insert, select

from activation.activators.base_activator import BaseOEMActivator
from core.sql_utils import get_async_session_maker
from db_models.vehicle import Vehicle, VehicleActivationHistory


class HighMobilityActivator(BaseOEMActivator):
    """Activator for High Mobility vehicles (Renault, Ford, Mercedes, Volvo)."""

    def get_oem_name(self) -> str:
        # HM handles multiple OEMs
        return "high-mobility"

    def get_vehicles_for_activation(self) -> pd.DataFrame:
        """Filter vehicles for HM OEMs that need activation changes."""
        return self.vehicle_with_command[
            self.vehicle_with_command["oem"].isin(
                ["renault", "ford", "mercedes-benz", "mercedes", "volvo-cars"]
            )
        ][["vin", "oem", "activation_requested_status", "activation_status"]].copy()

    def get_vehicles_for_processing(self) -> pd.DataFrame:
        """Filter vehicles for HM OEMs that need processing."""
        return self.vehicle_to_process[
            self.vehicle_to_process["oem"].isin(
                ["renault", "ford", "mercedes-benz", "mercedes", "volvo-cars"]
            )
        ]

    async def _get_vehicle_status_hm(self, data: list[dict], vin: str) -> dict | None:
        """Get vehicle status from HM API data."""
        # Get ALL entries for this VIN (there can be multiple)
        vehicle_entries = [v for v in data if v.get("vin") == vin]

        if not vehicle_entries:
            return None

        # Collect all changelog entries from ALL vehicle entries for this VIN
        all_changelog_entries = []

        for vehicle in vehicle_entries:
            changelog = vehicle.get("changelog", [])
            for entry in changelog:
                if isinstance(entry, dict) and "timestamp" in entry:
                    try:
                        # Parse ISO format timestamp and add VIN reference
                        entry_copy = entry.copy()
                        entry_copy["datetime_obj"] = datetime.fromisoformat(
                            entry["timestamp"].replace("Z", "+00:00")
                        )
                        entry_copy["vehicle_status"] = vehicle.get(
                            "status"
                        )  # Keep track of which vehicle entry this came from
                        all_changelog_entries.append(entry_copy)
                    except (ValueError, AttributeError) as e:
                        logging.warning(
                            f"Failed to parse timestamp for {vin}: {entry.get('timestamp')} - {e}"
                        )
                        continue

        if not all_changelog_entries:
            # No valid changelog entries found, use the most recent vehicle entry
            latest_vehicle = vehicle_entries[-1]
            return {
                "status": latest_vehicle.get("status"),
                "changelog_status": None,
                "reason": None,
            }

        # Sort ALL changelog entries by datetime (most recent first)
        sorted_all_changelog = sorted(
            all_changelog_entries,
            key=lambda x: x["datetime_obj"],
            reverse=True,
        )

        most_recent = sorted_all_changelog[0]
        reason = most_recent.get("reason")
        changelog_status = most_recent.get("status")
        vehicle_status = most_recent.get("vehicle_status")

        return {
            "status": vehicle_status,  # Status from the vehicle entry with most recent changelog
            "changelog_status": changelog_status,  # Most recent changelog entry status
            "reason": reason,
        }

    def _handle_status_equal(self, vehicle: Vehicle, values: dict) -> list[dict]:
        """Handle vehicles where current status equals desired status."""
        history_entries = []

        if values["status_desired"]:
            history_entries.append(
                self._update_vehicle_and_create_history(
                    vehicle, True, "ACTIVATED", None
                )
            )
        elif values["reason"] == "pending":
            history_entries.append(
                self._update_vehicle_and_create_history(
                    vehicle, False, "ACTIVATION PENDING", values["reason"]
                )
            )
        elif values["reason"] == "rejected":
            # Two history entries: failed activation + deactivation
            vehicle.activation_status = False
            vehicle.activation_status_message = "ACTIVATION FAILED"
            vehicle.activation_requested_status = False

            history_entries.append(
                self._create_history_entry(
                    vehicle,
                    False,
                    "ACTIVATION FAILED",
                    values["reason"],
                    override_requested_status=True,
                )
            )
            history_entries.append(
                self._create_history_entry(
                    vehicle, False, "DEACTIVATED", "", override_requested_status=False
                )
            )
        else:
            history_entries.append(
                self._update_vehicle_and_create_history(
                    vehicle, False, "ACTIVATION FAILED", values["reason"]
                )
            )

        return history_entries

    def _handle_activation_result(
        self, vehicle: Vehicle, status: dict | None, vin_dict: dict
    ) -> dict | None:
        """Handle activation result and return history entry."""
        if status:
            if status["status"] == "approved":
                return self._update_vehicle_and_create_history(
                    vehicle, True, "ACTIVATED", None
                )
            elif status["status"] == "pending":
                return self._update_vehicle_and_create_history(
                    vehicle, False, "ACTIVATION PENDING", status["status"]
                )
            else:
                return self._update_vehicle_and_create_history(
                    vehicle, False, "ACTIVATION FAILED", status["status"]
                )
        elif vin_dict["status"] == "error":
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION FAILED", vin_dict.get("description")
            )
        return None

    def _handle_deactivation_result(
        self, vehicle: Vehicle, vin: str, success: bool
    ) -> dict:
        """Handle deactivation result and return history entry."""
        if not success:
            return self._update_vehicle_and_create_history(
                vehicle, True, "DEACTIVATION FAILED", None
            )
        else:
            logging.info(f"Successfully deactivated High Mobility vehicle: {vin}")
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    async def activate(self) -> None:
        """Process High Mobility vehicle activation/deactivation."""
        df_hm = self.get_vehicles_for_activation()

        if df_hm.empty:
            self.logger.info("No High Mobility vehicles need activation changes")
            return

        df_hm["eligibility"] = False

        async with (
            get_async_session_maker()() as db,
            aiohttp.ClientSession() as session,
        ):
            data = await self.api.get_all_status(session)

            # Build status dictionary
            vin_groups = {}
            for vehicle in data:
                vin = vehicle["vin"]
                if vin not in vin_groups:
                    vin_groups[vin] = []
                vin_groups[vin].append(vehicle)

            status_dict = {
                vin: {
                    "status": vehicles[-1]["status"] == "approved",
                    "status_hm": vehicles[-1]["status"],
                }
                for vin, vehicles in vin_groups.items()
            }

            # Check for missing VINs
            missing_vins = [
                vin for vin in df_hm["vin"].tolist() if vin not in status_dict
            ]
            if missing_vins:
                logging.warning(
                    f"HM: The following VINs are in the database but NOT in the HM API response: {missing_vins}"
                )

            # Batch eligibility checks
            eligibility_tasks = [
                (idx, row["vin"], row["oem"])
                for idx, row in df_hm.iterrows()
                if row["activation_requested_status"]
            ]

            if eligibility_tasks:
                async with aiohttp.ClientSession() as session:
                    eligibility_results = await asyncio.gather(
                        *[
                            self.api.get_eligibility(vin, oem, session)
                            for _, vin, oem in eligibility_tasks
                        ]
                    )
                    for (idx, _, _), result in zip(
                        eligibility_tasks, eligibility_results, strict=True
                    ):
                        df_hm.at[idx, "eligibility"] = bool(result)

            # Categorize vehicles
            to_activate = {}
            to_deactivate = {}
            status_equal = {}

            for idx, row in df_hm.iterrows():
                vin = row["vin"]
                current_status_info = status_dict.get(
                    vin, {"status": False, "status_hm": "unknown"}
                )
                desired_status = row["activation_requested_status"]

                entry = {
                    "oem": row["oem"],
                    "status_desired": desired_status,
                    "current_status": current_status_info["status"],
                    "reason": current_status_info["status_hm"],
                    "eligibility": df_hm.at[idx, "eligibility"],
                }

                if not current_status_info["status"] and desired_status:
                    to_activate[vin] = entry
                elif current_status_info["status"] and not desired_status:
                    to_deactivate[vin] = entry
                elif current_status_info["status"] == desired_status:
                    status_equal[vin] = entry

            # Batch load all vehicles
            all_vins = list(
                set(
                    list(status_equal.keys())
                    + list(to_activate.keys())
                    + list(to_deactivate.keys())
                )
            )

            result = await db.execute(select(Vehicle).where(Vehicle.vin.in_(all_vins)))
            vehicles_dict = {v.vin: v for v in result.scalars().all()}

            history_inserts = []

            # Process status_equal vehicles
            logging.info("HM Status update started")
            for vin, values in status_equal.items():
                vehicle = vehicles_dict.get(vin)
                if vehicle:
                    history_inserts.extend(self._handle_status_equal(vehicle, values))

            # Process activations
            logging.info("HM Activation started")
            if to_activate:
                async with aiohttp.ClientSession() as session:
                    results_activation = await self.api.create_clearance_batch(
                        to_activate, session
                    )

                await asyncio.sleep(3)
                async with aiohttp.ClientSession() as session:
                    data = await self.api.get_all_status(session)

                for vin_dict in results_activation["vehicles"]:
                    vehicle = vehicles_dict.get(vin_dict["vin"])
                    if not vehicle:
                        continue

                    status = await self._get_vehicle_status_hm(data, vin_dict["vin"])
                    history_entry = self._handle_activation_result(
                        vehicle, status, vin_dict
                    )
                    if history_entry:
                        history_inserts.append(history_entry)

            # Process deactivations
            logging.info("HM Deactivation started")
            if to_deactivate:
                for vin in to_deactivate:
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        continue

                    async with aiohttp.ClientSession() as session:
                        success = await self.api.delete_clearance(vin, session)

                    history_inserts.append(
                        self._handle_deactivation_result(vehicle, vin, success)
                    )

            # Batch commit
            await db.flush()

            if history_inserts:
                stmt = insert(VehicleActivationHistory).values(history_inserts)
                await db.execute(stmt)

            await db.commit()
            logging.info(
                f"Batch processed {len(all_vins)} vehicles with {len(history_inserts)} history records"
            )
