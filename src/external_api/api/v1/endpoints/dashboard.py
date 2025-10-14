from collections.abc import AsyncGenerator

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.security import (
    get_current_user_with_fleet,
    get_current_user_with_fleet_id,
)
from external_api.db.session import get_db
from external_api.schemas.dashboard import DashboardCrud
from external_api.schemas.user import UserWithFleet

router = APIRouter()


def get_user_dependency(fleet_id: str):
    async def inner(fleet_id=fleet_id):
        return await get_current_user_with_fleet_id(fleet_id=fleet_id)

    return inner


@router.get("/kpis", include_in_schema=False)
async def kpis(
    db=Depends(get_db),
    Make: str = Query(None, description="The brands"),
    Country: str = Query(None, description="The country"),
    Fleets: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    fleet_ids_list = (
        Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    )
    country_list = (
        Country.split(",")
        if Country and "," in Country
        else [Country]
        if Country
        else None
    )
    brands_list = Make.split(",") if Make and "," in Make else [Make] if Make else None
    response = await DashboardCrud().kpis(
        user.fleet_ids, brands_list, country_list, fleet_ids_list, pinned_vehicles, db
    )
    return response


@router.get("/scatter_plot_brands", include_in_schema=False)
async def scatter_plot_brands(
    db=Depends(get_db),
    Make: str = Query(None, description="The brands"),
    Fleets: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    fleet_ids_list = (
        Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    )
    brands_list = Make.split(",") if Make and "," in Make else [Make] if Make else None
    response = await DashboardCrud().scatter_plot_brands(
        user.fleet_ids, brands_list, fleet_ids_list, pinned_vehicles, db
    )
    return response


@router.get("/scatter_plot_regions", include_in_schema=False)
async def scatter_plot_regions(
    db=Depends(get_db),
    Country: str = Query(None, description="The country"),
    Fleets: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    fleet_ids_list = (
        Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    )
    country_list = (
        Country.split(",")
        if Country and "," in Country
        else [Country]
        if Country
        else None
    )
    response = await DashboardCrud().scatter_plot_regions(
        user.fleet_ids, country_list, fleet_ids_list, pinned_vehicles, db
    )
    return response


@router.get("/individual/kpis", include_in_schema=False)
async def individual_kpis(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().individual_kpis(fleet_id, db)
    return response


@router.get("/individual/range_soh", include_in_schema=False)
async def range_soh(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
    type: str = Query(None, description="The type"),
):
    response = await DashboardCrud().range_soh(fleet_id, type, db)
    return response


@router.get("/individual/new_vehicles", include_in_schema=False)
async def new_vehicles(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    period: str = Query(None, description="The period"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().new_vehicles(fleet_id, period, db)
    return response


@router.get("/individual/table_brand", include_in_schema=False)
async def table_brand(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    filter: str = Query(None, description="The filter"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().table_brand(fleet_id, filter, db)
    return response


@router.get("/filters", include_in_schema=False)
async def filters(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().filter(
        base_fleet=user.fleet_ids, fleet_id=fleet_id, db=db
    )
    return response


@router.get("/search/{vin}", include_in_schema=False)
async def search_vin(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().search_vin(vin, user.fleet_ids, db)
    return response


@router.get("/global_table", include_in_schema=False)
async def global_table(
    db=Depends(get_db),
    Make: str = Query(None, description="The brands"),
    Country: str = Query(None, description="The country"),
    Fleets: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    fleet_ids_list = (
        Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    )
    country_list = (
        Country.split(",")
        if Country and "," in Country
        else [Country]
        if Country
        else None
    )
    brands_list = Make.split(",") if Make and "," in Make else [Make] if Make else None
    response = await DashboardCrud().global_table(
        user.fleet_ids, brands_list, country_list, fleet_ids_list, pinned_vehicles, db
    )
    return response


@router.get("/individual/trendline_brands", include_in_schema=False)
async def trendline_brands(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    brand: str = Query(None, description="The brand"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().trendline_brands(fleet_id, brand, db)
    return response


@router.get("/individual/brands", include_in_schema=False)
async def brands(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().brands(fleet_id, db)
    return response


@router.get("/individual/get_soh_by_groups", include_in_schema=False)
async def get_soh_by_groups(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    group: str = Query(..., description="The group"),
    page: int = Query(1, description="The page"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().get_soh_by_groups(fleet_id, group, page, db)
    return response


@router.get("/individual/get_extremum_soh", include_in_schema=False)
async def get_extremum_soh(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    brand: str = Query(None, description="The brand"),
    page: int | None = Query(
        None,
        description="Page number (1-based), None for page and page_size sends all data",
    ),
    page_size: int | None = Query(
        None, description="Items per page, None for page and page_size sends all data"
    ),
    extremum: str = Query("Worst", description="The extremum"),
    sorting_column: str = Query(None, description="The sorting column"),
    sorting_order: str = Query(None, description="The sorting order"),
    user: UserWithFleet = Depends(get_current_user_with_fleet()),
):
    response = await DashboardCrud().get_extremum_soh(
        fleet_id, brand, page, page_size, extremum, sorting_column, sorting_order, db
    )
    return response


async def get_user(email: str, db: AsyncSession) -> dict | None:
    query = text("""
        SELECT
            u.*,
            uf.fleet_id
        FROM "user" u
        LEFT JOIN "user_fleet" uf ON u.id = uf.user_id
        WHERE u.email = :email
    """)
    result = await db.execute(query, {"email": email})
    user = result.mappings().first()
    if user:
        user_dict = dict(user)
        # If no fleet_id is found, set it to None
        user_dict["fleet_id"] = user_dict.get("fleet_id")
        return user_dict
    return None


async def get_db(schema: str | None = None) -> AsyncGenerator[AsyncSession, None]:
    """Get a database session with optional schema."""
    async with get_db() as session:
        if schema:
            await session.execute(text(f"SET search_path TO {schema}"))
        yield session

