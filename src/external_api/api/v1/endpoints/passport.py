from fastapi import APIRouter, Body, Depends, Path

from core.sql_utils import get_async_db
from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleets,
)
from external_api.schemas.user import GetCurrentUser
from external_api.services.passport.passport import (
    get_charging_cycles,
    get_estimated_range,
    get_fleet_id_of_vin,
    get_graph_data,
    get_infos,
    get_kpis_additional,
    get_pinned_vehicle,
    post_pin_vehicle,
)
from external_api.services.passport.price_forecast import get_price_forecast

router = APIRouter()


@router.get("/is_vin_in_fleets/{vin}", include_in_schema=False)
async def is_vin_in_fleets(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_id = await get_fleet_id_of_vin(vin, db)
    return {
        "is_in_fleets": fleet_id in [fleet.id for fleet in user.fleets]
        if fleet_id and user.fleets
        else False
    }


@router.get("/graph/{vin}", include_in_schema=False)
async def graph(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_graph_data(vin, db)
    return response


@router.get("/infos/{vin}", include_in_schema=False)
async def infos(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_infos(vin, db)
    return response


@router.get("/estimated_range/{vin}")
async def estimated_range(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_estimated_range(vin, db)
    return response


@router.get("/kpis_additional/{vin}", include_in_schema=False)
async def kpis_additional(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_kpis_additional(vin, db)
    return response


@router.get("/charging-cycles/{vin}", include_in_schema=False)
async def charging_cycles(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_charging_cycles(vin, db)
    return response


@router.post("/pin_vehicle/{vin}", include_in_schema=False)
async def pin_vehicle(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    is_pinned: bool = Body(..., description="The is_pinned"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await post_pin_vehicle(vin, is_pinned, db)
    return response


@router.get("/pinned_vehicle/{vin}", include_in_schema=False)
async def pinned_vehicle(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_pinned_vehicle(vin, db)
    return response


@router.get("/price_forecast/{vin}", include_in_schema=False)
async def price_forecast(
    db=Depends(get_async_db),
    vin: str = Path(..., description="The vin"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    response = await get_price_forecast(vin, db)

    return response
