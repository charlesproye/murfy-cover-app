from fastapi import APIRouter, Depends, Path, Query

from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleet,
)
from external_api.db.session import get_db
from external_api.schemas.user import GetCurrentUser
from external_api.services.dashboard import (
    get_brands,
    get_extremum_vehicles,
    get_filter,
    get_global_table,
    get_individual_kpis,
    get_kpis,
    get_last_timestamp_with_data,
    get_new_vehicles,
    get_range_soh,
    get_scatter_plot_brands,
    get_scatter_plot_regions,
    get_search_vin,
    get_soh_by_groups,
    get_table_brand,
    get_trendline_brand,
)

router = APIRouter()


@router.get("/last_timestamp_with_data", include_in_schema=False)
async def last_timestamp_with_data(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_last_timestamp_with_data(fleet_id, db)
    return response


@router.get("/kpis", include_in_schema=False)
async def kpis(
    db=Depends(get_db),
    Make: str = Query(None, description="The brands"),
    Country: str = Query(None, description="The country"),
    _: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    # fleets_input_list = (
    #     Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    # )
    country_list = (
        Country.split(",")
        if Country and "," in Country
        else [Country]
        if Country
        else None
    )
    brands_list = Make.split(",") if Make and "," in Make else [Make] if Make else None
    response = await get_kpis(
        user.fleets, brands_list, country_list, pinned_vehicles, db
    )
    return response


@router.get("/scatter_plot_brands", include_in_schema=False)
async def scatter_plot_brands(
    db=Depends(get_db),
    Make: str = Query(None, description="The brands"),
    Fleets: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    fleets_input_list = (
        Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    )
    brands_list = Make.split(",") if Make and "," in Make else [Make] if Make else None
    response = await get_scatter_plot_brands(
        user.fleets, brands_list, fleets_input_list, pinned_vehicles, db
    )
    return response


@router.get("/scatter_plot_regions", include_in_schema=False)
async def scatter_plot_regions(
    db=Depends(get_db),
    Country: str = Query(None, description="The country"),
    Fleets: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    fleets_input_list = (
        Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    )
    country_list = (
        Country.split(",")
        if Country and "," in Country
        else [Country]
        if Country
        else None
    )
    response = await get_scatter_plot_regions(
        user.fleets, country_list, fleets_input_list, pinned_vehicles, db
    )
    return response


@router.get("/individual/kpis", include_in_schema=False)
async def individual_kpis(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_individual_kpis(fleet_id, db)
    return response


@router.get("/individual/range_soh", include_in_schema=False)
async def range_soh(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
    type: str = Query(None, description="The type"),
):
    response = await get_range_soh(fleet_id, type, db)
    return response


@router.get("/individual/new_vehicles", include_in_schema=False)
async def new_vehicles(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    period: str = Query(None, description="The period"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_new_vehicles(fleet_id, period, db)
    return response


@router.get("/individual/table_brand", include_in_schema=False)
async def table_brand(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    filter: str = Query(None, description="The filter"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_table_brand(fleet_id, filter, db)
    return response


@router.get("/filters", include_in_schema=False)
async def filters(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_filter(base_fleet=user.fleets, fleet_id=fleet_id, db=db)
    return response


@router.get("/search/{vin}", include_in_schema=False)
async def search_vin(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    fleets_ids = [fleet.id for fleet in user.fleets] if user.fleets else []
    response = await get_search_vin(vin, fleets_ids, db)
    return response


@router.get("/global_table", include_in_schema=False)
async def global_table(
    db=Depends(get_db),
    Make: str = Query(None, description="The brands"),
    Country: str = Query(None, description="The country"),
    _: str = Query(None, description="The fleets"),
    pinned_vehicles: bool = Query(False, description="The pinned vehicles"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    # fleet_ids_list = (
    #     Fleets.split(",") if Fleets and "," in Fleets else [Fleets] if Fleets else None
    # )
    country_list = (
        Country.split(",")
        if Country and "," in Country
        else [Country]
        if Country
        else None
    )
    brands_list = Make.split(",") if Make and "," in Make else [Make] if Make else None
    response = await get_global_table(
        user.fleets, brands_list, country_list, pinned_vehicles, db
    )
    return response


@router.get("/individual/trendline_brand", include_in_schema=False)
async def trendline_brand(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    brand: str = Query(None, description="The brand"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_trendline_brand(fleet_id, db, brand)
    return response


@router.get("/individual/brands", include_in_schema=False)
async def brands(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_brands(fleet_id, db)
    return response


@router.get("/individual/soh_by_groups", include_in_schema=False)
async def soh_by_groups(
    db=Depends(get_db),
    fleet_id: str = Query(..., description="The fleet id"),
    group: str = Query(..., description="The group"),
    page: int = Query(1, description="The page"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_soh_by_groups(fleet_id, group, page, db)
    return response


@router.get("/individual/extremum_vehicles", include_in_schema=False)
async def extremum_vehicles(
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
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await get_extremum_vehicles(
        fleet_id, brand, page, page_size, extremum, sorting_column, sorting_order, db
    )
    return response
