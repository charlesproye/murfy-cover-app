"""BMW OEM Activator - Handles activation and processing for BMW vehicles."""

import asyncio
import json
import logging

import aiohttp
import pandas as pd
from sqlalchemy import insert, select

from activation.activators.base_activator import BaseOEMActivator
from activation.config.mappings import mapping_vehicle_type
from core.models.make import MakeEnum
from core.sql_utils import get_async_session_maker
from db_models.vehicle import Vehicle, VehicleActivationHistory


class BMWActivator(BaseOEMActivator):
    """Activator for BMW vehicles."""

    def get_oem_name(self) -> str:
        return MakeEnum.bmw.value

    async def _activate_bmw(
        self, session: aiohttp.ClientSession, vin: str
    ) -> tuple[bool, str | None]:
        """Activate a BMW vehicle using BMW's API."""
        try:
            license_plate = ""
            end_date = ""

            if self.vehicle_with_command is not None:
                vehicle_info = self.vehicle_with_command[
                    self.vehicle_with_command["vin"] == vin
                ]
                if not vehicle_info.empty:
                    license_plate = (
                        str(vehicle_info["licence_plate"].iloc[0])
                        if "licence_plate" in vehicle_info.columns
                        else ""
                    )
                    end_date = (
                        str(vehicle_info["end_of_contract"].iloc[0])
                        if "end_of_contract" in vehicle_info.columns
                        else ""
                    )

                    license_plate = "" if pd.isna(license_plate) else license_plate
                    end_date = "" if pd.isna(end_date) else end_date

                    logging.info(
                        f"Found vehicle info - VIN: {vin}, License Plate: {license_plate}, End Date: {end_date}"
                    )
                else:
                    logging.warning(
                        f"No vehicle info found in fleet_info for VIN: {vin}"
                    )
            else:
                logging.warning(
                    "vehicle_command_df is None, using empty values for license_plate and end_date"
                )

            payload = {
                "vin": vin,
                "licence_plate": license_plate,
                "note": "",
                "contract": {
                    "end_date": end_date,
                },
            }

            status_code, result = await self.api.create_clearance(payload, session)

            if not status_code:
                return False, result

            logging.info("Create clearance response")

            if isinstance(result, str):
                result = json.loads(result) if result.strip() else {}

            if status_code in [200, 201, 204]:
                fleet_success, fleet_error = await self._add_to_fleet(vin, session)
                if not fleet_success:
                    return False, fleet_error
                return True, None
            elif "message" in result:
                if result["message"] == "Too Many Requests":
                    asyncio.sleep(1)
                    return await self._activate_bmw(session, vin)
                else:
                    pass
            elif (
                status_code == 403
                and result["logErrorId"] == "BMWFD_VEHICLE_ALREADY_EXISTS"
            ):
                logging.info(f"BMW vehicle {vin} already added")

                return True, None
            elif (
                status_code == 412
                and result["logErrorId"] == "BMWFD_VEHICLE_NOT_ALLOWED"
            ):
                if "BMW HU version too old" in result["message"]:
                    return False, "BMW HU version too old - header unit"
                else:
                    return False, result["message"]

            return False, json.dumps(result) if isinstance(result, dict) else result

        except Exception as e:
            logging.error(f"Error activating BMW vehicle {vin}: {e!s}")
            return False, str(e)

    async def _add_to_fleet(
        self, vin: str, session: aiohttp.ClientSession
    ) -> tuple[bool, str | None]:
        """Add a vehicle to the appropriate fleet based on ownership."""
        try:
            vehicle_info = self.vehicle_with_command[
                self.vehicle_with_command["vin"] == vin
            ]
            target_fleet_name = (
                str(vehicle_info["owner"].iloc[0])
                if "owner" in vehicle_info.columns
                else "bib"
            )

            status_code, result = await self.api.get_fleets(session)

            if status_code != 200:
                error_msg = f"Failed to get fleets: HTTP {status_code}"
                return False, error_msg

            target_fleet_id = None
            for fleet in result.get("fleets", []):
                if fleet.get("name", "").lower() == target_fleet_name.lower():
                    target_fleet_id = fleet["fleet_id"]
                    break

            if not target_fleet_id:
                for fleet in result.get("fleets", []):
                    if fleet.get("name", "").lower() == "bib":
                        target_fleet_id_bib = fleet["fleet_id"]
                        break
                warning_msg = (
                    f"Fleet {target_fleet_name} not found, adding to bib fleet"
                )
                logging.warning(warning_msg)
                target_fleet_id = target_fleet_id_bib

            status_code, result = await self.api.add_vehicle_to_fleet(
                target_fleet_id, vin, session
            )

            if isinstance(result, str):
                result = json.loads(result) if result.strip() else {}

            if status_code in [200, 201, 204]:
                logging.info(
                    f"Successfully added vehicle {vin} to {target_fleet_name} fleet"
                )
                return True, None
            if status_code == 422:
                logging.info(f"Vehicle {vin} already in fleet")
                return True, None
            if status_code == 429:
                if result["message"] == "Too Many Requests":
                    asyncio.sleep(1)
                    return await self._add_to_fleet(vin, session)

            error_msg = f"Failed to add vehicle to fleet: HTTP {status_code}, {result}"
            logging.error(error_msg)
            return False, error_msg

        except Exception as e:
            error_msg = f"Error adding vehicle {vin} to fleet: {e!s}"
            logging.error(error_msg)
            return False, error_msg

    async def process(self) -> None:
        """Process BMW vehicles to enrich their data."""
        bmw_df = self.get_vehicles_for_processing()

        if bmw_df.empty:
            self.logger.info("No BMW vehicles to process")
            return

        async with (
            aiohttp.ClientSession() as session,
            get_async_session_maker()() as db,
        ):
            model_existing = await self._get_existing_models(db)

            for _, row in bmw_df.iterrows():
                vin = row["vin"]
                result = await db.execute(select(Vehicle).where(Vehicle.vin == vin))
                vehicle = result.scalar_one_or_none()

                model_name, model_type = await self.api.get_data(vin, session)

                model_id = mapping_vehicle_type(
                    model_type,
                    row["make"],
                    model_name,
                    model_existing,
                )

                vehicle.vehicle_model_id = model_id
                vehicle.is_processed = True
                await db.commit()

    async def _get_existing_models(self, db) -> pd.DataFrame:
        """Get existing vehicle models from database."""
        from sqlalchemy import select

        from db_models.company import Make
        from db_models.vehicle import Battery, VehicleModel

        query = (
            select(
                VehicleModel.model_name,
                VehicleModel.id,
                VehicleModel.type,
                VehicleModel.commissioning_date,
                VehicleModel.end_of_life_date,
                Make.make_name,
                Battery.capacity,
            )
            .join(Make, VehicleModel.make_id == Make.id, isouter=True)
            .join(Battery, VehicleModel.battery_id == Battery.id, isouter=True)
        )

        result = await db.execute(query)
        rows = result.fetchall()

        # Convert to DataFrame for mapping_vehicle_type function
        model_existing = pd.DataFrame(
            [
                {
                    "model_name": row.model_name,
                    "id": str(row.id),
                    "type": row.type,
                    "commissioning_date": row.commissioning_date,
                    "end_of_life_date": row.end_of_life_date,
                    "make_name": row.make_name,
                    "capacity": float(row.capacity) if row.capacity else None,
                }
                for row in rows
            ]
        )
        return model_existing

    async def _handle_status_equal(
        self,
        vehicle: Vehicle,
        vin: str,
        current_state: bool,
        in_fleet: bool,
        session: aiohttp.ClientSession,
    ) -> dict:
        """Handle vehicles where current status equals desired status."""
        if not in_fleet and current_state:
            await self._add_to_fleet(vin, session)

        logging.info(f"BMW vehicle {vin} already in desired state: {current_state}")

        if current_state:
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    async def _handle_activation(
        self, vehicle: Vehicle, vin: str, session: aiohttp.ClientSession
    ) -> list[dict]:
        """Handle vehicle activation and return history entries."""
        history_entries = []
        success, api_detail = await self._activate_bmw(session, vin)

        if success:
            history_entries.append(
                self._update_vehicle_and_create_history(
                    vehicle, True, "ACTIVATED", None
                )
            )
        elif api_detail and "header unit" in api_detail:
            logging.info(
                f"BMW vehicle {vin} header unit version is too old to have sufficient data frequency"
            )
            # Two history entries: failed activation + automatic deactivation
            history_entries.append(
                self._create_history_entry(
                    vehicle,
                    False,
                    "ACTIVATION FAILED",
                    api_detail,
                    override_requested_status=True,
                )
            )
            history_entries.append(
                self._create_history_entry(
                    vehicle, False, "DEACTIVATED", "", override_requested_status=False
                )
            )
        else:
            logging.info(f"BMW vehicle {vin} activation failed")
            history_entries.append(
                self._update_vehicle_and_create_history(
                    vehicle, False, "ACTIVATION FAILED", api_detail
                )
            )

        return history_entries

    async def _handle_deactivation(
        self, vehicle: Vehicle, vin: str, session: aiohttp.ClientSession
    ) -> dict:
        """Handle vehicle deactivation and return history entry."""
        deactivation_success = await self.api.deactivate(vin, session)

        if deactivation_success:
            # Verify deactivation status
            current_state, _ = await self.api.check_vehicle_status(vin, session)

            if not current_state:
                logging.info(f"BMW vehicle {vin} deactivated successfully")
                return self._update_vehicle_and_create_history(
                    vehicle, False, "DEACTIVATED", None
                )
            else:
                logging.info(
                    f"BMW vehicle {vin} deactivation seemed successful but vehicle is still active"
                )
                return self._update_vehicle_and_create_history(
                    vehicle, False, "DEACTIVATION FAILED", None
                )
        else:
            logging.info(
                f"BMW vehicle {vin} deactivation failed - API returned failure"
            )
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATION FAILED", None
            )

    async def activate(self) -> None:
        """Process BMW vehicle activation/deactivation."""
        df_bmw = self.get_vehicles_for_activation()

        if df_bmw.empty:
            self.logger.info("No BMW vehicles need activation changes")
            return

        async with (
            get_async_session_maker()() as db,
            aiohttp.ClientSession() as session,
        ):
            # Batch load all vehicles
            all_vins = df_bmw["vin"].tolist()
            result = await db.execute(select(Vehicle).where(Vehicle.vin.in_(all_vins)))
            vehicles_dict = {v.vin: v for v in result.scalars().all()}

            # Batch check all vehicle statuses
            status_tasks = [
                self.api.check_vehicle_status(vin, session) for vin in all_vins
            ]
            status_results = await asyncio.gather(*status_tasks)
            status_dict = {
                vin: {"current_state": current_state, "in_fleet": in_fleet}
                for vin, (current_state, in_fleet) in zip(
                    all_vins, status_results, strict=True
                )
            }

            history_inserts = []

            # Process each vehicle
            for _, row in df_bmw.iterrows():
                vin = row["vin"]
                desired_state = row["activation_requested_status"]
                vehicle = vehicles_dict.get(vin)

                if not vehicle:
                    logging.warning(f"BMW vehicle {vin} not found in database")
                    continue

                status_info = status_dict.get(vin)
                if not status_info:
                    logging.warning(f"BMW vehicle {vin} status check failed")
                    continue

                current_state = status_info["current_state"]
                in_fleet = status_info["in_fleet"]

                # Route to appropriate handler
                if desired_state == current_state:
                    entry = await self._handle_status_equal(
                        vehicle, vin, current_state, in_fleet, session
                    )
                    if entry:
                        history_inserts.append(entry)

                elif desired_state:
                    entries = await self._handle_activation(vehicle, vin, session)
                    history_inserts.extend(entries)

                else:
                    entry = await self._handle_deactivation(vehicle, vin, session)
                    if entry:
                        history_inserts.append(entry)

            # Batch commit
            await db.flush()

            if history_inserts:
                stmt = insert(VehicleActivationHistory).values(history_inserts)
                await db.execute(stmt)

            await db.commit()
            logging.info(
                f"BMW: Batch processed {len(all_vins)} vehicles with {len(history_inserts)} history records"
            )
