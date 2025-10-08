from sqlalchemy.ext.asyncio import AsyncSession

from external_api.services.passport.passport import (
    get_charging_cycles,
    get_download_rapport,
    get_estimated_range,
    get_graph_data,
    get_infos,
    get_kpis,
    get_kpis_additional,
    get_pinned_vehicle,
    pin_vehicle,
)
from external_api.services.passport.price_forecast import get_price_forecast


class PassportCrud:
    async def get_kpis(self, vin: str, db: AsyncSession | None = None):
        return await get_kpis(vin, db)

    async def get_graph_data(
        self, vin: str, period: str, db: AsyncSession | None = None
    ):
        return await get_graph_data(vin, period, db)

    async def get_infos(self, vin: str, db: AsyncSession | None = None):
        return await get_infos(vin, db)

    async def get_estimated_range(self, vin: str, db: AsyncSession | None = None):
        return await get_estimated_range(vin, db)

    async def get_kpis_additional(self, vin: str, db: AsyncSession | None = None):
        return await get_kpis_additional(vin, db)

    async def get_download_rapport(self, vin: str, db: AsyncSession | None = None):
        return await get_download_rapport(vin, db)

    async def get_charging_cycles(self, vin: str, db: AsyncSession | None = None):
        return await get_charging_cycles(vin, db)

    async def pin_vehicle(
        self, vin: str, is_pinned: bool, db: AsyncSession | None = None
    ):
        return await pin_vehicle(vin, is_pinned, db)

    async def get_pinned_vehicle(self, vin: str, db: AsyncSession | None = None):
        return await get_pinned_vehicle(vin, db)

    async def get_price_forecast(self, vin: str, db: AsyncSession | None = None):
        return await get_price_forecast(vin, db)

