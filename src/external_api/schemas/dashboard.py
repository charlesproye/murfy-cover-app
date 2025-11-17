from pydantic import UUID4
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.services.dashboard import (
    get_brands,
    get_extremum_soh,
    get_filter,
    get_global_table,
    get_individual_kpis,
    get_kpis,
    get_last_timestamp_with_data,
    get_new_vehicles,
    get_range_soh,
    get_scatter_plot_brands,
    get_scatter_plot_regions,
    get_soh_by_groups,
    get_table_brand,
    get_trendline_brand,
    search_vin,
)


class DashboardCrud:
    async def get_last_timestamp_with_data(
        self, fleet_id: str, db: AsyncSession | None = None
    ):
        return await get_last_timestamp_with_data(fleet_id, db)

    async def kpis(
        self,
        fleets: list[str],
        brands: list[str],
        regions: list[str],
        pinned_vehicles: bool,
        db: AsyncSession | None = None,
    ):
        return await get_kpis(fleets, brands, regions, pinned_vehicles, db)

    async def scatter_plot_brands(
        self,
        fleets: list[str],
        brands: list[str],
        fleets_input_list: list[str],
        pinned_vehicles: bool,
        db: AsyncSession | None = None,
    ):
        return await get_scatter_plot_brands(
            fleets, brands, fleets_input_list, pinned_vehicles, db
        )

    async def scatter_plot_regions(
        self,
        fleets: list[str],
        regions: list[str],
        fleets_input_list: list[str],
        pinned_vehicles: bool,
        db: AsyncSession | None = None,
    ):
        return await get_scatter_plot_regions(
            fleets, regions, fleets_input_list, pinned_vehicles, db
        )

    async def filter(
        self, base_fleet: list[str], fleet_id: str, db: AsyncSession | None = None
    ):
        return await get_filter(base_fleet, fleet_id, db)

    async def individual_kpis(self, fleet_id: str, db: AsyncSession | None = None):
        return await get_individual_kpis(fleet_id, db)

    async def range_soh(self, fleet_id: str, type: str, db: AsyncSession | None = None):
        return await get_range_soh(fleet_id, type, db)

    async def new_vehicles(
        self, fleet_id: str, period: str, db: AsyncSession | None = None
    ):
        return await get_new_vehicles(fleet_id, period, db)

    async def table_brand(
        self, fleet_id: str, filter: str, db: AsyncSession | None = None
    ):
        return await get_table_brand(fleet_id, filter, db)

    async def search_vin(
        self, vin: str, fleets_ids: list[UUID4], db: AsyncSession | None = None
    ):
        return await search_vin(vin, fleets_ids, db)

    async def global_table(
        self,
        fleets: list[str],
        brands: list[str],
        regions: list[str],
        pinned_vehicles: bool,
        db: AsyncSession | None = None,
    ):
        return await get_global_table(fleets, brands, regions, pinned_vehicles, db)

    async def trendline_brand(
        self, fleet_id: str, brand: str, db: AsyncSession | None = None
    ):
        return await get_trendline_brand(fleet_id=fleet_id, brand=brand, db=db)

    async def brands(self, fleet_id: str, db: AsyncSession | None = None):
        return await get_brands(fleet_id, db)

    async def get_soh_by_groups(
        self, fleet_id: str, group: str, page: int, db: AsyncSession | None = None
    ):
        return await get_soh_by_groups(fleet_id, group, page, db)

    async def get_extremum_soh(
        self,
        fleet_id: str,
        brand: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
        extremum: str | None = None,
        sorting_column: str | None = None,
        sorting_order: str | None = None,
        db: AsyncSession | None = None,
    ):
        return await get_extremum_soh(
            fleet_id,
            brand,
            page,
            page_size,
            extremum,
            sorting_column,
            sorting_order,
            db,
        )
