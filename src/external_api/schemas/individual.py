from sqlalchemy.ext.asyncio import AsyncSession

from external_api.services.individual import (
    get_all_consumption,
    get_all_fast_charge,
    get_all_pinned_vehicles,
)


class IndividualCrud:
    async def get_all_pinned_vehicles(
        self, fleet_id: str, page: int, limit: int, db: AsyncSession | None = None
    ):
        return await get_all_pinned_vehicles(fleet_id, page, limit, db)

    async def get_all_fast_charge(
        self, fleet_id: str, page: int, limit: int, db: AsyncSession | None = None
    ):
        return await get_all_fast_charge(fleet_id, page, limit, db)

    async def get_all_consumption(
        self, fleet_id: str, page: int, limit: int, db: AsyncSession | None = None
    ):
        return await get_all_consumption(fleet_id, page, limit, db)

