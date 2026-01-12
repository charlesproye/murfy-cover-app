"""Tesla OEM Activator - Handles activation and processing for Tesla vehicles."""

import asyncio
import logging
import uuid
from collections import defaultdict

import aiohttp
from sqlalchemy import func, insert, select

from activation.activators.base_activator import BaseOEMActivator
from activation.config.mappings import MAKE_MAPPING, OEM_MAPPING, TESLA_MODEL_MAPPING
from core.models.make import MakeEnum
from core.sql_utils import get_async_session_maker
from db_models.company import Make, Oem
from db_models.vehicle import Vehicle, VehicleActivationHistory, VehicleModel


class TeslaActivator(BaseOEMActivator):
    """Activator for Tesla vehicles."""

    def get_oem_name(self) -> str:
        return MakeEnum.tesla.value

    def _handle_status_equal(
        self, vehicle: Vehicle, vin: str, current_state: bool
    ) -> dict:
        """Handle vehicles where current status equals desired status."""
        if current_state:
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )

    async def _handle_activation_result(
        self, vehicle: Vehicle, vin: str, fleet_id: str, session: aiohttp.ClientSession
    ) -> dict:
        """Check activation result and return history entry."""
        current_state, message = await self.api.check_vehicle_status(
            vin, fleet_id, session
        )

        if current_state:
            return self._update_vehicle_and_create_history(
                vehicle, True, "ACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, False, "ACTIVATION PENDING", message
            )

    async def _handle_deactivation(
        self, vehicle: Vehicle, vin: str, fleet_id: str, session: aiohttp.ClientSession
    ) -> dict:
        """Handle vehicle deactivation and return history entry."""
        success, error_message = await self.api.deactivate_vehicles(
            vin, fleet_id, session
        )

        if success:
            return self._update_vehicle_and_create_history(
                vehicle, False, "DEACTIVATED", None
            )
        else:
            return self._update_vehicle_and_create_history(
                vehicle, True, "DEACTIVATION FAILED", error_message
            )

    async def activate(self) -> None:
        """Process Tesla vehicle activation/deactivation."""
        df_tesla = self.get_vehicles_for_activation()

        if df_tesla.empty:
            self.logger.info("No Tesla vehicles need activation changes")
            return

        async with (
            get_async_session_maker()() as db,
            aiohttp.ClientSession() as session,
        ):
            # Fetch all tokens first
            await self.api.fetch_all_tokens(session, db)

            # Batch check all vehicle statuses
            status_check_tasks = [
                self.api.check_vehicle_status(row["vin"], row["fleet_id"], session)
                for _, row in df_tesla.iterrows()
            ]
            status_results = await asyncio.gather(*status_check_tasks)

            # Create status mapping
            status_dict = {
                row["vin"]: {
                    "current_state": status_results[idx][0],
                    "message": status_results[idx][1],
                    "fleet_id": row["fleet_id"],
                    "desired_state": row["activation_requested_status"],
                }
                for idx, (_, row) in enumerate(df_tesla.iterrows())
            }

            # Categorize vehicles
            vin_to_activate: list[tuple[str, str]] = []
            vin_to_deactivate: list[tuple[str, str]] = []
            vin_in_desired_state: list[tuple[str, bool, str]] = []

            for vin, info in status_dict.items():
                if info["current_state"] == info["desired_state"]:
                    vin_in_desired_state.append(
                        (vin, info["current_state"], info["fleet_id"])
                    )
                elif info["current_state"] and not info["desired_state"]:
                    vin_to_deactivate.append((vin, info["fleet_id"]))
                elif info["desired_state"] and not info["current_state"]:
                    vin_to_activate.append((vin, info["fleet_id"]))

            # Batch load all vehicles
            all_vins = list(status_dict.keys())
            result = await db.execute(select(Vehicle).where(Vehicle.vin.in_(all_vins)))
            vehicles_dict = {v.vin: v for v in result.scalars().all()}

            history_inserts = []

            # Process vehicles already in desired state
            if vin_in_desired_state:
                for vin, current_state, fleet_id in vin_in_desired_state:
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(f"Tesla vehicle {vin} not found in database")
                        continue

                    entry = self._handle_status_equal(vehicle, vin, current_state)
                    if entry:
                        history_inserts.append(entry)

            # Batch activate vehicles
            if vin_to_activate:
                await self._activate_tesla_batch(vin_to_activate, session)

                # Check activation results in parallel
                activation_check_tasks = [
                    self._handle_activation_result(
                        vehicles_dict.get(vin), vin, fleet_id, session
                    )
                    for vin, fleet_id in vin_to_activate
                    if vehicles_dict.get(vin)
                ]
                activation_results = await asyncio.gather(*activation_check_tasks)
                history_inserts.extend([r for r in activation_results if r])

            # Process deactivations (no batch available - sequential)
            if vin_to_deactivate:
                for vin, fleet_id in vin_to_deactivate:
                    vehicle = vehicles_dict.get(vin)
                    if not vehicle:
                        logging.warning(f"Tesla vehicle {vin} not found in database")
                        continue

                    entry = await self._handle_deactivation(
                        vehicle, vin, fleet_id, session
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
                f"Tesla: Batch processed {len(all_vins)} vehicles with {len(history_inserts)} history records"
            )

    async def _activate_tesla_batch(
        self, vin_to_activate: list[tuple[str, str]], session: aiohttp.ClientSession
    ):
        """Activate Tesla vehicles in batches of 50."""
        grouped = defaultdict(list)
        for vin, fleet_id in vin_to_activate:
            grouped[fleet_id].append(vin)
        grouped = dict(grouped)

        for fleet_id, vins in grouped.items():
            if len(vins) > 50:
                for i in range(0, len(vins), 50):
                    batch = vins[i : i + 50]
                    await self.api.activate_vehicles(batch, fleet_id, session)
            else:
                await self.api.activate_vehicles(vins, fleet_id, session)

    async def process(self) -> None:
        """Process Tesla vehicles to enrich their data."""
        tesla_df = self.get_vehicles_for_processing()

        if tesla_df.empty:
            self.logger.info("No Tesla vehicles to process")
            return

        async with (
            aiohttp.ClientSession() as session,
            get_async_session_maker()() as db,
        ):
            await self.api.fetch_all_tokens(session, db)
            for _, row in tesla_df.iterrows():
                vin = row["vin"]
                result = await db.execute(select(Vehicle).where(Vehicle.vin == vin))
                vehicle_obj = result.scalar_one_or_none()

                model_code = vin[3]
                model_name = TESLA_MODEL_MAPPING.get(model_code, None)

                # Skip if model_name cannot be determined
                if model_name is None:
                    logging.warning(f"Could not determine model name for VIN {vin}")
                    continue

                fleet_id = uuid.UUID(row["fleet_id"])

                # Get model info from API
                (
                    api_version,
                    api_model_type,
                ) = await self.api.get_vehicle_options(
                    session, vin, fleet_id, model_name
                )

                (
                    warranty_km,
                    warranty_date,
                    _,
                ) = await self.api.get_warranty_info(session, vin, fleet_id)

                # Create/get model and related records
                model_id = await self._get_or_create_tesla_model(
                    db,
                    model_name,
                    api_model_type,
                    api_version,
                    row["make"],
                    row["oem"],
                    warranty_km,
                    warranty_date,
                )

                # Update vehicle model with Tesla call
                vehicle_obj.vehicle_model_id = model_id
                if model_id:
                    vehicle_obj.is_processed = True
                await db.commit()
                logging.info(f"Updated Tesla vehicle in DB VIN: {vin}")

            # NB : Cas très particulier MTY13B qui a été monté avec deux types de batteries
            if tesla_df.empty:
                return

            # Get last processed vehicle (for the special case check)
            last_row = tesla_df.iloc[-1]
            vin = last_row["vin"]

            if (
                vin[9] == "P"
                and vin[7] == "S"
                and str(model_id) == "4c43f13c-2246-45ea-aa33-a4f956e7237a"
            ):
                vehicle_obj.vehicle_model_id = uuid.UUID(
                    "4c43f13c-2246-45ea-aa33-a4f956e7237b"
                )
                await db.commit()

    async def _get_or_create_tesla_model(
        self,
        db,
        model_name: str,
        model_type: str,
        version: str,
        make: str,
        oem: str,
        warranty_km: int,
        warranty_date: str,
    ) -> uuid.UUID:
        """Get a Tesla model if it exists then update it, or create it if it doesn't exist."""

        # Search for existing model by version
        result = await db.execute(
            select(VehicleModel).where(
                func.lower(VehicleModel.version) == version.lower()
            )
        )
        vehicle_model = result.scalar_one_or_none()

        # Get OEM and Make IDs
        oem_id = await self._get_oem(db, oem)
        make_id = await self._get_or_create_make(db, make, oem_id)

        if vehicle_model:
            # Update existing model
            if warranty_km and not vehicle_model.warranty_km:
                vehicle_model.warranty_km = warranty_km
            if warranty_date and not vehicle_model.warranty_date:
                vehicle_model.warranty_date = warranty_date

            await db.commit()
            logging.info(f"Updated existing Tesla model with version {version}")
            return vehicle_model.id
        else:
            # Create new model
            vehicle_model = VehicleModel(
                id=uuid.uuid4(),
                model_name=model_name,
                type=model_type,
                version=version,
                make_id=make_id,
                oem_id=oem_id,
                warranty_km=warranty_km,
                warranty_date=warranty_date,
            )
            db.add(vehicle_model)
            await db.commit()
            await db.refresh(vehicle_model)
            logging.info(f"Created new Tesla model with version {version}")
            return vehicle_model.id

    async def _get_oem(self, db, oem_raw: str) -> uuid.UUID:
        """Get OEM record."""
        oem_lower = OEM_MAPPING.get(oem_raw, oem_raw.lower())

        result = await db.execute(
            select(Oem).where(func.lower(Oem.oem_name) == oem_lower)
        )
        oem = result.scalar_one_or_none()

        if not oem:
            raise ValueError(f"OEM {oem_lower} not found in database")

        return oem.id

    async def _get_or_create_make(
        self, db, make_raw: str, oem_id: uuid.UUID
    ) -> uuid.UUID:
        """Get or create Make record."""
        make_lower = MAKE_MAPPING.get(make_raw, make_raw.lower())

        result = await db.execute(
            select(Make).where(func.lower(Make.make_name) == make_lower)
        )
        make = result.scalar_one_or_none()

        if not make:
            make = Make(id=uuid.uuid4(), make_name=make_lower, oem_id=oem_id)
            db.add(make)
            await db.commit()
            await db.refresh(make)
            logging.info(f"Created new make: {make_lower}")

        return make.id
