import logging

import fastapi
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core import numpy_utils
from db_models.vehicle import Vehicle, VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.core.utils import check_rate_limit
from external_api.db.session import get_db
from external_api.schemas.flash import SOHWithTrendline
from external_api.schemas.user import GetCurrentUser

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/{vin}/soh",
    response_model=SOHWithTrendline,
    summary="Get SoH estimated by vehicle",
    description="Returns the SoH trendline for a vehicle by VIN for a given odometer.",
)
async def get_model_soh_trendline(
    request: fastapi.Request,
    vin: str = Path(..., description="VIN of the vehicle"),
    odometer: int = Query(..., ge=0, description="Odometer in km"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> SOHWithTrendline:
    await check_rate_limit(
        vin=vin, endpoint=request.scope["route"].path, user=user, db=db
    )

    # Get vehicle and its model from VIN
    vehicle_query = (
        select(Vehicle, VehicleModel)
        .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
        .where(Vehicle.vin == vin)
    )

    vehicle_result = await db.execute(vehicle_query)
    vehicle_row = vehicle_result.first()

    if not vehicle_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vehicle with VIN {vin} not found",
        )

    _, vehicle_model = vehicle_row

    if not vehicle_model.trendline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Model for VIN {vin} has no trendline",
        )

    # Get trendline data
    query = select(
        VehicleModel.trendline["trendline"].as_string().label("trendline"),
        VehicleModel.trendline_min["trendline"].as_string().label("trendline_min"),
        VehicleModel.trendline_max["trendline"].as_string().label("trendline_max"),
        VehicleModel.commissioning_date,
        VehicleModel.end_of_life_date,
        VehicleModel.id,
    ).where(VehicleModel.id == vehicle_model.id)

    trendline_result = await db.execute(query)
    trendline = trendline_result.fetchone()

    if not trendline:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Model not found",
        )

    soh = round(numpy_utils.numpy_safe_eval(trendline.trendline, x=odometer), 2)

    return SOHWithTrendline(
        model_id=vehicle_model.id,
        trendline_mean=trendline.trendline,
        soh=soh,
        odometer=odometer,
        trendline_min=trendline.trendline_min,
        trendline_max=trendline.trendline_max,
    )
