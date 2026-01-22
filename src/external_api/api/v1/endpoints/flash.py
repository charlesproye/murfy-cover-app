import logging
import uuid

import fastapi
import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.numpy_utils import numpy_safe_eval
from core.sql_utils import get_async_db
from core.trendlines_utils import get_flash_trendlines
from db_models import VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.schemas.flash import SOHWithTrendline
from external_api.schemas.user import GetCurrentUser

logger = logging.getLogger(__name__)

router = APIRouter()


def generate_trendline_points(
    trendline: str, odometers: list[int]
) -> list[tuple[int, float]]:
    soh_values = numpy_safe_eval(trendline, x=np.array(odometers))

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
    db: AsyncSession = Depends(get_async_db),
) -> SOHWithTrendline:
    # Get trendline data
    query = (
        select(VehicleModel)
        .where(VehicleModel.id == model_id)
        .where(
            (VehicleModel.trendline_bib.is_not(None))
            | (VehicleModel.trendline_oem.is_not(None))
        )
    )

    result = await db.execute(query)
    vehicle_model = result.scalar_one_or_none()

    if not vehicle_model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model not found or model has no trendline",
        )

    odometers = list(range(0, 150000, 30000))

    trendlines = get_flash_trendlines(vehicle_model=vehicle_model)

    trendline_min_points = generate_trendline_points(trendlines.minimum, odometers)
    trendline_max_points = generate_trendline_points(trendlines.maximum, odometers)
    soh = float(numpy_safe_eval(expression=trendlines.main, x=odometer))

    return SOHWithTrendline(
        model_id=model_id,
        soh=soh,
        source=trendlines.source,
        odometer=odometer,
        trendline_min=trendline_min_points,
        trendline_max=trendline_max_points,
    )
