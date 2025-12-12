"""Endpoints for vehicle data access"""

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db_models.vehicle import Battery, Make, VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.db.session import get_db
from external_api.schemas.static_data import (
    BatteryModelData,
    ModelData,
    ModelTrendline,
    ModelType,
    VehicleModelData,
)
from external_api.schemas.user import GetCurrentUser

logger = logging.getLogger(__name__)

router = APIRouter()


async def check_model_exists(model_id: uuid.UUID, db: AsyncSession) -> bool:
    query = select(VehicleModel).where(VehicleModel.id == model_id)
    result = await db.execute(query)

    if not result.fetchone():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Vehicle model with id {model_id} does not exist",
        )

    return model_id


def is_tesla_vin(vin: str) -> bool:
    """
    Detects if a VIN belongs to a Tesla vehicle.

    Tesla VINs typically start with:
    - 5YJ for vehicles manufactured in the United States (Model S, 3, X, Y)
    - 7SA for vehicles manufactured in China
    - LRW for some Chinese vehicles
    - SFZ for some European vehicles (Berlin)

    Args:
        vin: Vehicle Identification Number

    Returns:
        bool: True if the VIN corresponds to a Tesla vehicle, False otherwise
    """
    if len(vin) < 3:
        return False

    tesla_prefixes = ["5YJ", "7SA", "LRW", "SFZ", "XP7"]
    return vin[:3].upper() in tesla_prefixes


@router.get(
    "/models",
    response_model=list[ModelType],
    summary="Get all models",
    description="Returns all electric vehicles models with their commissioning and end of life dates and whether Bib provides a trendline. If you want to get only the models with a trendline, you can set the `has_trendline` query parameter to `true`.",
)
async def get_models(
    has_trendline: bool | None = Query(None),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> list[ModelType]:
    query = (
        select(
            VehicleModel.id,
            Make.make_name,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            VehicleModel.trendline,
        )
        .select_from(VehicleModel)
        .join(Make, VehicleModel.make_id == Make.id)
        .join(Battery, VehicleModel.battery_id == Battery.id)
    )

    if has_trendline is True:
        query = query.where(VehicleModel.trendline.is_not(None))
    elif has_trendline is False:
        query = query.where(VehicleModel.trendline.is_(None))

    vehicules_query = await db.execute(query)
    vehicules = vehicules_query.fetchall()

    return [
        ModelType(
            model_id=vehicule.id,
            make=vehicule.make_name,
            model_name=vehicule.model_name,
            model_type=vehicule.type,
            commissioning_date=vehicule.commissioning_date,
            end_of_life_date=vehicule.end_of_life_date,
            has_trendline=vehicule.trendline is not None,
        )
        for vehicule in vehicules
    ]


@router.get(
    "/models/{model_id}/data",
    response_model=ModelData,
    summary="Get data by model",
    description="Returns the available static data for a specific model.",
)
async def get_model_data(
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    model_id: uuid.UUID = Path(..., description="Model ID"),
    db: AsyncSession = Depends(get_db),
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
)
async def get_model_trendline(
    model_id: uuid.UUID = Path(..., description="Model ID (UUID)"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
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
            VehicleModel.trendline["trendline"].as_string().label("trendline_mean"),
            VehicleModel.trendline_min["trendline"].as_string().label("trendline_min"),
            VehicleModel.trendline_max["trendline"].as_string().label("trendline_max"),
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            VehicleModel.version,
        )
        .where(VehicleModel.id == model_id)
        .where(VehicleModel.trendline.is_not(None))
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
        trendline_mean=record.trendline_mean,
        trendline_min=record.trendline_min,
        trendline_max=record.trendline_max,
        comment=comment,
    )
