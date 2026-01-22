"""Endpoints for vehicle data access"""

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.sql_utils import get_async_db
from db_models import Battery, Make, VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.schemas.static_data import (
    BatteryModelData,
    ModelData,
    ModelTrendline,
    VehicleModelData,
)
from external_api.schemas.user import GetCurrentUser

logger = logging.getLogger(__name__)

router = APIRouter()


async def check_model_exists(model_id: uuid.UUID, db: AsyncSession) -> None:
    query = select(VehicleModel).where(VehicleModel.id == model_id)
    result = await db.execute(query)

    if not result.fetchone():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Vehicle model with id {model_id} does not exist",
        )


@router.get(
    "/models/{model_id}/data",
    response_model=ModelData,
    summary="Get data by model",
    description="Returns the available static data for a specific model.",
)
async def get_model_data(
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    model_id: uuid.UUID = Path(..., description="Model ID"),
    db: AsyncSession = Depends(get_async_db),
) -> ModelData:
    await check_model_exists(model_id, db)

    query = (
        select(VehicleModel, Make, Battery)
        .join(Make, VehicleModel.make_id == Make.id)
        .join(Battery, VehicleModel.battery_id == Battery.id)
        .where(VehicleModel.id == model_id)
    )
    result = await db.execute(query)
    row = result.first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Vehicle model with id {model_id} does not exist",
        )

    vm, make, battery = row

    return ModelData(
        model_id=vm.id,
        make=make.make_name,
        model_name=vm.model_name,
        model_type=vm.type,
        version=vm.version,
        vehicle=VehicleModelData.model_validate(vm),
        battery=BatteryModelData.model_validate(battery),
    )


@router.get(
    "/models/{model_id}/trendline",
    response_model=ModelTrendline,
    summary="Get trendline by model",
    description="Returns the trendline data for a specific model.",
    include_in_schema=False,
)
async def get_model_trendline(
    model_id: uuid.UUID = Path(..., description="Model ID (UUID)"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
) -> ModelTrendline:
    """
    Get trendline data for a specific model.

    Args:
        id: ID (UUID) of the model
        user: Current authenticated user
        db: Database session

    Returns:
        ModelTrendline containing the trendline information
    """

    await check_model_exists(model_id, db)

    stmt = (
        select(
            VehicleModel.id,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.version,
            VehicleModel.trendline_bib,
            VehicleModel.trendline_bib_min,
            VehicleModel.trendline_bib_max,
            VehicleModel.trendline_oem,
            VehicleModel.trendline_oem_min,
            VehicleModel.trendline_oem_max,
            VehicleModel.has_trendline_bib,
            VehicleModel.has_trendline_oem,
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            VehicleModel.version,
        )
        .where(VehicleModel.id == model_id)
        .where(
            (VehicleModel.trendline_bib.is_not(None))
            | (VehicleModel.trendline_oem.is_not(None))
        )
    )

    result = await db.execute(stmt)
    record = result.first()

    if not record:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Vehicle model with id {model_id} has no trendline",
        )

    if record.version is not None:
        comment = "This trendline is based on the version of the model"
    else:
        comment = ""

    return ModelTrendline(
        model_id=record.id,
        model_name=record.model_name,
        model_type=record.type,
        version=record.version,
        trendline_bib_mean=record.trendline_bib,
        trendline_bib_min=record.trendline_bib_min,
        trendline_bib_max=record.trendline_bib_max,
        trendline_oem=record.trendline_oem,
        trendline_oem_min=record.trendline_oem_min,
        trendline_oem_max=record.trendline_oem_max,
        comment=comment,
    )
