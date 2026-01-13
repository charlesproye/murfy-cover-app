"""Kia OEM Activator - Handles activation and processing for Kia vehicles."""

import logging

import aiohttp
from sqlalchemy import insert, select

from activation.activators.base_activator import BaseOEMActivator
from core.models.make import MakeEnum
from core.sql_utils import get_async_session_maker
from db_models.vehicle import Vehicle, VehicleActivationHistory


class KiaActivator(BaseOEMActivator):
    """Activator for Kia vehicles."""

    def get_oem_name(self) -> str:
        return MakeEnum.kia.value

    @staticmethod
    def safe_bool(val):
        """Convert None/NaN to empty string, keep True/False as-is."""
        if val is True:
            return True
        if val is False:
            return False
        return ""

    async def _get_current_states_kia(
        self, session: aiohttp.ClientSession, vins: list[str]
    ):
        """Get the current states of a list of vins."""

        current_states_vehicle = await self.api._get_status_consent_type(
            session, "vehicle"
        )

        current_states_dtc = await self.api._get_status_consent_type(session, "dtcInfo")

        merged = {}

        for vin in vins:
            merged[vin] = {"vehicle": False, "dtcInfo": False}

        for el in current_states_vehicle:
            if el["vin"] in merged:
                merged[el["vin"]]["vehicle"] = True
        for el in current_states_dtc:
            if el["vin"] in merged:
                merged[el["vin"]]["dtcInfo"] = True

        for vin in merged:
            merged[vin]["status_bool"] = bool(
                merged[vin]["vehicle"] and merged[vin]["dtcInfo"]
            )
            if merged[vin]["vehicle"] and (not merged[vin]["dtcInfo"]):
                merged[vin]["reason"] = "Package véhicule activé mais pas dtcInfo"
            elif merged[vin]["dtcInfo"] and (not merged[vin]["vehicle"]):
                merged[vin]["reason"] = "Package dtcInfo activé mais pas véhicule"
            elif (not merged[vin]["dtcInfo"]) and (not merged[vin]["vehicle"]):
                merged[vin]["reason"] = "Aucun package activé"
            else:
                merged[vin]["reason"] = ""

        return merged

    def _handle_status_equal(
        self, vehicle: Vehicle, vin: str, current_state: bool
    ) -> dict:
        """Handle vehicles where current status equals desired status."""
        logging.info(f"KIA vehicle {vin} already in desired state: {current_state}")

        if self.safe_bool(current_state):
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    def _handle_activation_result(
        self, vehicle: Vehicle, vin: str, state_info: dict | None
    ) -> dict:
        """Handle activation result and return history entry."""
        if not state_info:
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION FAILED", "VIN not eligible for data sharing"
            )

        if state_info["status_bool"]:
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION FAILED", state_info["reason"]
            )

    def _handle_deactivation_result(self, vehicle: Vehicle, vin: str) -> dict:
        """Handle deactivation result and return history entry."""
        logging.info(f"KIA vehicle {vin} deactivated successfully")
        return self._update_vehicle_and_create_history(
            vehicle, False, "DEACTIVATED", None
        )

    async def activate(self) -> None:
        """Process KIA vehicle activation/deactivation."""
        df_kia = self.get_vehicles_for_activation()

        if df_kia.empty:
            self.logger.info("No KIA vehicles need activation changes")
            return

        async with (
            get_async_session_maker()() as db,
            aiohttp.ClientSession() as session,
        ):
            # Prepare desired states with activation_status_message
            desired_states = {
                vin: {
                    "desired_state": desired_state,
                    "activation_status_message": status_msg,
                }
                for vin, desired_state, status_msg in zip(
                    df_kia["vin"].tolist(),
                    df_kia["activation_requested_status"].tolist(),
                    df_kia["activation_status_message"].tolist(),
                    strict=False,
                )
            }

            # Batch get current states for ALL vehicles
            merged = await self._get_current_states_kia(
                session, list(desired_states.keys())
            )

            merged_states = {
                vin: {
                    "desired_state": desired_states[vin]["desired_state"],
                    "current_state": merged[vin]["status_bool"],
                    "current_state_status": merged[vin]["reason"],
                    "activation_status_message": desired_states[vin][
                        "activation_status_message"
                    ],
                }
                for vin in merged
            }

            # Categorize vehicles
            vins_in_desired_state = {
                vin: info
                for vin, info in merged_states.items()
                if (info["desired_state"] == info["current_state"])
                and (info["activation_status_message"] != "DEACTIVATION REQUESTED")
            }

            vins_to_activate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is True and info["current_state"] is False
            ]

            vins_to_deactivate = [
                vin
                for vin, info in merged_states.items()
                if (info["desired_state"] is False and info["current_state"] is True)
                or (
                    info["desired_state"] is False
                    and info["current_state"] is False
                    and info["activation_status_message"] == "DEACTIVATION REQUESTED"
                )
            ]

            # Batch load ALL vehicles at once
            all_vins = list(merged_states.keys())
            result = await db.execute(select(Vehicle).where(Vehicle.vin.in_(all_vins)))
            vehicles_dict = {v.vin: v for v in result.scalars().all()}

            history_inserts = []

            # Process vehicles in desired state
            logging.info("KIA STATUS UPDATE: Starting")
            for vin, info in vins_in_desired_state.items():
                vehicle = vehicles_dict.get(vin)
                if not vehicle:
                    logging.warning(f"KIA vehicle {vin} not found in database")
                    continue

                entry = self._handle_status_equal(vehicle, vin, info["current_state"])
                if entry:
                    history_inserts.append(entry)

            # Process activations
            logging.info("KIA ACTIVATION: Starting")
            logging.info(f"Vins to activate: {vins_to_activate}")

            if vins_to_activate:
                # Process in batches of 50 (API limit)
                for i in range(0, len(vins_to_activate), 50):
                    batch = vins_to_activate[i : i + 50]
                    await self.api._activate_vins(session, batch)

                    # Check activation results
                    activated_current_states = await self._get_current_states_kia(
                        session, batch
                    )

                    for vin in batch:
                        vehicle = vehicles_dict.get(vin)
                        if not vehicle:
                            logging.warning(f"KIA vehicle {vin} not found in database")
                            continue

                        state_info = activated_current_states.get(vin)
                        entry = self._handle_activation_result(vehicle, vin, state_info)
                        if entry:
                            history_inserts.append(entry)
            else:
                logging.info("No vins to activate")

            # Process deactivations
            logging.info("\nKIA DEACTIVATION: Starting")
            logging.info(f"Vins to deactivate: {vins_to_deactivate}")

            if vins_to_deactivate:
                # Process in batches of 50
                for i in range(0, len(vins_to_deactivate), 50):
                    batch = vins_to_deactivate[i : i + 50]
                    await self.api.delete_consent(session, batch)

                    for vin in batch:
                        vehicle = vehicles_dict.get(vin)
                        if not vehicle:
                            logging.warning(f"KIA vehicle {vin} not found in database")
                            continue

                        entry = self._handle_deactivation_result(vehicle, vin)
                        if entry:
                            history_inserts.append(entry)
            else:
                logging.info("No vins to deactivate")

            # Batch commit
            await db.flush()

            if history_inserts:
                stmt = insert(VehicleActivationHistory).values(history_inserts)
                await db.execute(stmt)

            await db.commit()
            logging.info(
                f"KIA: Batch processed {len(all_vins)} vehicles with {len(history_inserts)} history records"
            )
