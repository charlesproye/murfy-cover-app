"""Stellantis OEM Activator - Handles activation and processing for Stellantis vehicles."""

import logging

import aiohttp
from sqlalchemy import insert, select

from activation.activators.base_activator import BaseOEMActivator
from core.models.make import MakeEnum
from core.sql_utils import get_async_session_maker
from db_models.vehicle import Vehicle, VehicleActivationHistory


class StellantisActivator(BaseOEMActivator):
    """Activator for Stellantis vehicles."""

    def get_oem_name(self) -> str:
        return MakeEnum.stellantis.value

    def _handle_status_equal(self, vehicle: Vehicle, vin: str, values: dict) -> dict:
        """Handle vehicles where current status equals desired status."""
        if values["status_desired"]:
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    async def _handle_activation(
        self, vehicle: Vehicle, vin: str, entry: dict, session: aiohttp.ClientSession
    ) -> dict:
        """Handle vehicle activation and return history entry."""
        if not entry["eligibility"]:
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION FAILED", "Not eligible"
            )

        status_code, result = await self.api.activate(vin, session)

        if status_code in [200, 201, 204]:
            if result["status"] == "activated":
                return self._update_vehicle_and_create_history(
                    vehicle, True, "ACTIVATED", None
                )
            else:
                return self._update_vehicle_and_create_history(
                    vehicle, False, "ACTIVATION PENDING", result["status"]
                )
        elif status_code == 409:
            logging.info(f"Stellantis vehicle {vin} activation already in progress")
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION PENDING", entry["reason"]
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION FAILED", None
            )

    async def _handle_deactivation(
        self, vehicle: Vehicle, vin: str, entry: dict, session: aiohttp.ClientSession
    ) -> dict:
        """Handle vehicle deactivation and return history entry."""
        status_code, error_msg = await self.api.deactivate(entry["id"], session)

        if status_code in [200, 204]:
            logging.info(f"Stellantis vehicle {vin} deactivated successfully")
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )
        else:
            logging.info(
                f"Failed to deactivate Stellantis vehicle {vin}: HTTP {status_code} - {error_msg}"
            )
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATION FAILED", error_msg
            )

    async def activate(self) -> None:
        """Process Stellantis vehicle activation/deactivation."""
        df_stellantis = self.get_vehicles_for_activation()

        if df_stellantis.empty:
            self.logger.info("No Stellantis vehicles need activation changes")
            return

        vin_list = df_stellantis["vin"].tolist()

        async with (
            get_async_session_maker()() as db,
            aiohttp.ClientSession() as session,
        ):
            # Batch fetch eligibilities and statuses
            eligibilities = await self.api.is_eligible_batch(vin_list, session)
            statuses = await self.api.get_status_batch(vin_list, session)

            # Categorize vehicles
            to_activate = {}
            to_deactivate = {}
            status_equal = {}

            for _, row in df_stellantis.iterrows():
                vin = row["vin"]
                desired_status = row["activation_requested_status"]
                current_status = statuses.get(vin, {}).get("status", "") == "activated"
                reason = statuses.get(vin, {}).get("status", "")
                contract_id = statuses.get(vin, {}).get("id", "")

                entry = {
                    "eligibility": eligibilities.get(vin, False),
                    "status_desired": desired_status,
                    "current_status": current_status,
                    "reason": reason,
                    "id": contract_id,
                }

                if not current_status and desired_status:
                    to_activate[vin] = entry
                elif (current_status and not desired_status) or (
                    not desired_status
                    and row["activation_status_message"] == "DEACTIVATION REQUESTED"
                ):
                    to_deactivate[vin] = entry
                elif current_status == desired_status:
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

            # Process vehicles in desired state
            if status_equal:
                for vin, values in status_equal.items():
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(
                            f"Stellantis vehicle {vin} not found in database"
                        )
                        continue

                    entry = self._handle_status_equal(vehicle, vin, values)
                    if entry:
                        history_inserts.append(entry)

            # Process activations (note: API doesn't support batch, must be sequential)
            if to_activate:
                for vin, entry_data in to_activate.items():
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(
                            f"Stellantis vehicle {vin} not found in database"
                        )
                        continue

                    entry = await self._handle_activation(
                        vehicle, vin, entry_data, session
                    )
                    if entry:
                        history_inserts.append(entry)

            # Process deactivations (note: API doesn't support batch, must be sequential)
            if to_deactivate:
                for vin, entry_data in to_deactivate.items():
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(
                            f"Stellantis vehicle {vin} not found in database"
                        )
                        continue

                    entry = await self._handle_deactivation(
                        vehicle, vin, entry_data, session
                    )
                    if entry:
                        history_inserts.append(entry)

            # Batch commit
            await db.flush()

            if history_inserts:
                stmt = insert(VehicleActivationHistory).values(history_inserts)
                await db.execute(stmt)

            await db.commit()
            logging.info(
                f"Stellantis: Batch processed {len(all_vins)} vehicles with {len(history_inserts)} history records"
            )
