import logging
import uuid

import fastapi
import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core import numpy_utils
from db_models.vehicle import VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.db.session import get_db
from external_api.schemas.flash import SOHWithTrendline
from external_api.schemas.user import GetCurrentUser

logger = logging.getLogger(__name__)

router = APIRouter()


async def generate_trendline_points(
    trendline: str, odometers: list[int]
) -> list[tuple[int, float]]:
    soh_values = numpy_utils.numpy_safe_eval(trendline, x=np.array(odometers))

    return list(zip(odometers, soh_values, strict=True))


@router.get(
    "/{model_id}/soh",
    response_model=SOHWithTrendline,
    summary="Get SoH estimated by model_id and odometer",
    description="Returns the SoH estimation and min and max trendline points for a vehicle by model_id for a given odometer.",
)
async def get_model_soh_trendline(
    request: fastapi.Request,
    model_id: uuid.UUID = Path(..., description="Model ID (UUID)"),
    odometer: int = Query(..., ge=0, description="Odometer in km"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> SOHWithTrendline:
    # Get trendline data
    query = (
        select(
            VehicleModel.trendline["trendline"].as_string().label("trendline"),
            VehicleModel.trendline_min["trendline"].as_string().label("trendline_min"),
            VehicleModel.trendline_max["trendline"].as_string().label("trendline_max"),
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            VehicleModel.id,
        )
        .where(VehicleModel.id == model_id)
        .where(VehicleModel.trendline.is_not(None))
    )

    trendline_result = await db.execute(query)
    trendline = trendline_result.fetchone()

    if not trendline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model not found or model has no trendline",
        )

    odometers = list(range(0, 150000, 30000))
    trendline_min = await generate_trendline_points(trendline.trendline_min, odometers)
    trendline_max = await generate_trendline_points(trendline.trendline_max, odometers)

    soh = round(numpy_utils.numpy_safe_eval(trendline.trendline, x=odometer), 2)

    return SOHWithTrendline(
        model_id=model_id,
        soh=soh,
        odometer=odometer,
        trendline_min=trendline_min,
        trendline_max=trendline_max,
    )
