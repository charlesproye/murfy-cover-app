from fastapi import APIRouter, Depends, Path, Query

from core.sql_utils import get_async_db
from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleets,
)
from external_api.schemas.user import GetCurrentUser
from external_api.services.dashboard import (
    get_extremum_vehicles,
    get_individual_kpis,
    get_last_timestamp_with_data,
    get_new_vehicles,
    get_range_soh,
    get_search_vin,
    get_soh_by_groups,
    get_table_brand,
    get_trendline_brand,
)

router = APIRouter()


@router.get("/last_timestamp_with_data", include_in_schema=False)
async def last_timestamp_with_data(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_last_timestamp_with_data(fleet_ids, db)
    return response


@router.get("/kpis", include_in_schema=False)
async def individual_kpis(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_individual_kpis(
        [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets], db
    )
    return response


@router.get("/range_soh", include_in_schema=False)
async def range_soh(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
    type: str = Query(None, description="The type"),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_range_soh(fleet_ids, type, db)
    return response


@router.get("/new_vehicles", include_in_schema=False)
async def new_vehicles(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    period: str = Query(None, description="The period"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_new_vehicles(fleet_ids, period, db)
    return response


@router.get("/table_brand", include_in_schema=False)
async def table_brand(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    filter: str = Query(None, description="The filter"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_table_brand(fleet_ids, filter, db)
    return response


@router.get("/search/{vin}", include_in_schema=False)
async def search_vin(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleets_ids = [fleet.id for fleet in user.fleets] if user.fleets else []
    response = await get_search_vin(vin, fleets_ids, db)
    return response


@router.get("/trendline_brand", include_in_schema=False)
async def trendline_brand(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    brand: str = Query(None, description="The brand"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_trendline_brand(fleet_ids, db, brand)
    return response


@router.get("/soh_by_groups", include_in_schema=False)
async def soh_by_groups(
    db=Depends(get_async_db),
    fleet_id: str = Query(..., description="The fleet id"),
    group: str = Query(..., description="The group"),
    page: int = Query(1, description="The page"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_soh_by_groups(fleet_ids, group, page, db)
    return response


@router.get("/extremum_vehicles", include_in_schema=False)
async def extremum_vehicles(
    db=Depends(get_async_db),
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
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_extremum_vehicles(
        fleet_ids, brand, page, page_size, extremum, sorting_column, sorting_order, db
    )
    return response
