"""Renault OEM Activator - Handles activation and processing for Renault vehicles."""

from datetime import datetime

import aiohttp
import pandas as pd
from sqlalchemy import select

from activation.activators.base_activator import BaseOEMActivator
from activation.config.mappings import mapping_vehicle_type
from core.models.make import MakeEnum
from core.sql_utils import get_async_session_maker
from db_models.company import Make
from db_models.vehicle import Battery, Vehicle, VehicleModel


class RenaultActivator(BaseOEMActivator):
    """Activator for Renault vehicles."""

    def get_oem_name(self) -> str:
        return MakeEnum.renault.value

    async def activate(self) -> None:
        pass

    async def process(self) -> None:
        """Process Renault vehicles to enrich their data."""
        renault_df = self.get_vehicles_for_processing()

        if renault_df.empty:
            self.logger.info("No Renault vehicles to process")
            return

        async with (
            aiohttp.ClientSession() as session,
            get_async_session_maker()() as db,
        ):
            for _, row in renault_df.iterrows():
                vin = row["vin"]
                result = await db.execute(select(Vehicle).where(Vehicle.vin == vin))
                vehicle = result.scalar_one_or_none()

                (
                    model_name,
                    model_type,
                    version,
                    date,
                ) = await self.api.get_vehicle_info(session, vin)

                # Handle None values for model_type and version
                if model_type and version:
                    model_type = model_type + " " + version
                elif version:
                    model_type = version

                model_existing = await self._get_existing_models(db)

                model_id = mapping_vehicle_type(
                    model_type,
                    row["make"],
                    model_name,
                    model_existing,
                    sale_year=date if date else None,
                )

                # Update vehicle
                vehicle.vehicle_model_id = model_id
                # Convert date string to date object if it's a string
                if isinstance(date, str):
                    vehicle.start_date = datetime.fromisoformat(date).date()
                else:
                    vehicle.start_date = date
                vehicle.is_processed = True
                await db.commit()

    async def _get_existing_models(self, db) -> pd.DataFrame:
        """Get existing vehicle models from database."""
        from sqlalchemy import select

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
