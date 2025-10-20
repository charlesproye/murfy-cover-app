from fastapi import APIRouter, Body, Depends, Path, Query

from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleet,
)
from external_api.db.session import get_db
from external_api.schemas.passport import PassportCrud
from external_api.schemas.user import GetCurrentUser

router = APIRouter()


@router.get("/kpis/{vin}", include_in_schema=False)
async def kpis(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_kpis(vin, db)
    return response


@router.get("/graph/{vin}", include_in_schema=False)
async def graph(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    period: str = Query(..., description="The period"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_graph_data(vin, period, db)
    return response


@router.get("/infos/{vin}", include_in_schema=False)
async def infos(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_infos(vin, db)
    return response


@router.get("/estimated_range/{vin}")
async def estimated_range(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_estimated_range(vin, db)
    return response


@router.get("/kpis_additional/{vin}", include_in_schema=False)
async def kpis_additional(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_kpis_additional(vin, db)
    return response


@router.get("/download_rapport/{vin}", include_in_schema=False)
async def download_rapport(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_download_rapport(vin, db)
    return response


@router.get("/charging-cycles/{vin}", include_in_schema=False)
async def charging_cycles(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_charging_cycles(vin, db)
    return response


@router.post("/pin_vehicle/{vin}", include_in_schema=False)
async def pin_vehicle(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    is_pinned: bool = Body(..., description="The is_pinned"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().pin_vehicle(vin, is_pinned, db)
    return response


@router.get("/get_pinned_vehicle/{vin}", include_in_schema=False)
async def get_pinned_vehicle(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_pinned_vehicle(vin, db)
    return response


@router.get("/get_price_forecast/{vin}", include_in_schema=False)
async def get_price(
    db=Depends(get_db),
    vin: str = Path(..., description="The vin"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
):
    response = await PassportCrud().get_price_forecast(vin, db)
    return response

