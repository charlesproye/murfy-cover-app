"""Volkswagen OEM Activator - Handles activation and processing for Volkswagen vehicles."""

import logging

import aiohttp
from sqlalchemy import insert, select

from activation.activators.base_activator import BaseOEMActivator
from activation.config.config import REASON_MAPPING
from core.models.make import MakeEnum
from core.sql_utils import get_async_session_maker
from db_models.vehicle import Vehicle, VehicleActivationHistory


class VolkswagenActivator(BaseOEMActivator):
    """Activator for Volkswagen vehicles."""

    def get_oem_name(self) -> str:
        return MakeEnum.volkswagen.value

    @staticmethod
    def safe_bool(val):
        """Convert None/NaN to empty string, keep True/False as-is."""
        if val is True:
            return True
        if val is False:
            return False
        return ""

    def _handle_status_equal(
        self, vehicle: Vehicle, vin: str, current_state: bool
    ) -> dict:
        """Handle vehicles where current status equals desired status."""
        logging.info(
            f"Volkswagen vehicle {vin} already in desired state: {current_state}"
        )

        if self.safe_bool(current_state):
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    def _handle_activation_result(self, vehicle: Vehicle, vin: str, info: dict) -> dict:
        """Handle activation result and return history entry."""
        logging.info(
            f"Update Volkswagen vehicle that has just been activated {vin} : {info.get('status_bool')}"
        )

        if info.get("status_bool"):
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            # Map API status to readable message
            if info.get("status") in REASON_MAPPING["volkswagen"]:
                api_detail = REASON_MAPPING["volkswagen"][info.get("status")]
            else:
                api_detail = "Statut inconnu, voir avec l'équipe TECH pour le définir."

            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION FAILED", api_detail
            )

    def _handle_deactivation_result(
        self, vehicle: Vehicle, vin: str, info: dict
    ) -> dict:
        """Handle deactivation result and return history entry."""
        logging.info(
            f"Update Volkswagen vehicle that has just been deactivated {vin} : {info.get('status_bool')}"
        )

        if info.get("status_bool"):
            # Vehicle is still active - deactivation failed
            return self._update_vehicle_and_create_history(
                vehicle, True, "DEACTIVATION FAILED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    async def activate(self) -> None:
        """Process Volkswagen vehicle activation/deactivation."""
        df_vw = self.get_vehicles_for_activation()

        if df_vw.empty:
            self.logger.info("No Volkswagen vehicles need activation changes")
            return

        async with (
            get_async_session_maker()() as db,
            aiohttp.ClientSession() as session,
        ):
            # Prepare desired states
            desired_states = {
                vin: {"desired_state": desired_state}
                for vin, desired_state in zip(
                    df_vw["vin"].tolist(),
                    df_vw["activation_requested_status"].tolist(),
                    strict=False,
                )
            }

            # Batch check current states
            current_states = await self.api.check_vehicle_status(
                session, desired_states
            )

            # Merge states
            merged_states = {
                vin: {
                    "desired_state": desired_states[vin]["desired_state"],
                    "current_state": current_states.get(vin, {}).get("status_bool"),
                    "current_state_status": current_states.get(vin, {}).get("status"),
                    "reason": current_states.get(vin, {}).get("reason"),
                }
                for vin in desired_states
            }

            # Categorize vehicles
            vins_in_desired_state = {
                vin: info
                for vin, info in merged_states.items()
                if info["desired_state"] == info["current_state"]
            }

            vins_to_activate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is True and info["current_state"] is False
            ]

            vins_to_deactivate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is False and info["current_state"] is True
            ]

            # Batch load all vehicles
            all_vins = list(merged_states.keys())
            result = await db.execute(select(Vehicle).where(Vehicle.vin.in_(all_vins)))
            vehicles_dict = {v.vin: v for v in result.scalars().all()}

            history_inserts = []

            # Process vehicles in desired state
            logging.info("\nVOLKSWAGEN STATUS UPDATE: Starting")
            for vin, info in vins_in_desired_state.items():
                vehicle = vehicles_dict.get(vin)
                if not vehicle:
                    logging.warning(f"Volkswagen vehicle {vin} not found in database")
                    continue

                entry = self._handle_status_equal(vehicle, vin, info["current_state"])
                if entry:
                    history_inserts.append(entry)

            # Process activations
            logging.info("\nVOLKSWAGEN ACTIVATION: Starting")
            if vins_to_activate:
                await self.api.activate_vehicles(session, vins_to_activate)
                activated_current_states = await self.api.check_vehicle_status(
                    session, vins_to_activate
                )

                for vin, info in activated_current_states.items():
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(
                            f"Volkswagen vehicle {vin} not found in database"
                        )
                        continue

                    entry = self._handle_activation_result(vehicle, vin, info)
                    if entry:
                        history_inserts.append(entry)

            # Process deactivations
            logging.info("\nVOLKSWAGEN DEACTIVATION: Starting")
            if vins_to_deactivate:
                await self.api.deactivate_vehicles(session, vins_to_deactivate)
                deactivated_current_states = await self.api.check_vehicle_status(
                    session, vins_to_deactivate
                )

                for vin, info in deactivated_current_states.items():
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(
                            f"Volkswagen vehicle {vin} not found in database"
                        )
                        continue

                    entry = self._handle_deactivation_result(vehicle, vin, info)
                    if entry:
                        history_inserts.append(entry)

            # Batch commit
            await db.flush()

            if history_inserts:
                stmt = insert(VehicleActivationHistory).values(history_inserts)
                await db.execute(stmt)

            await db.commit()
            logging.info(
                f"Volkswagen: Batch processed {len(all_vins)} vehicles with {len(history_inserts)} history records"
            )
