from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from db_models.vehicle import FlashReportCombination, Make, User, VehicleModel
from external_api.core.utils import get_flash_report_user
from external_api.db.session import get_db
from external_api.schemas.flash_report import FlashReportFormType, VehicleSpecs
from external_api.schemas.static_data import (
    AllMakesModelsInfo,
    MakeInfo,
    ModelInfo,
    TypeInfo,
)
from external_api.services.api_pricing import log_api_call
from external_api.services.flash_report.flash_report import (
    get_flash_report_data,
    insert_combination,
    log_vin_decoded,
    send_email,
    send_vehicle_specs,
)

flash_report_router = APIRouter()


@flash_report_router.post("/vin-decoder")
async def decode_vin(
    request: Request,
    vin: str = Body(..., description="VIN to decode"),
    flash_report_user: User = Depends(get_flash_report_user),
    db: AsyncSession = Depends(get_db),
) -> VehicleSpecs:
    await log_api_call(
        vin=vin,
        endpoint=request.scope["route"].path,
        user_id=flash_report_user.id,
        db=db,
    )

    result = await send_vehicle_specs(vin, db)

    if result.make and not result.has_trendline:
        await log_vin_decoded(db, vin, result.make, result.model)
    return result


@flash_report_router.post("/send-email")
async def send_report_email(
    request: Request,
    data: FlashReportFormType = Body(...),
    db: AsyncSession = Depends(get_db),
    flash_report_user: User = Depends(get_flash_report_user),
) -> dict[str, str]:
    await log_api_call(
        vin=data.vin,
        endpoint=request.scope["route"].path,
        user_id=flash_report_user.id,
        db=db,
    )

    token = await insert_combination(
        vin=data.vin,
        make=data.make,
        model=data.model,
        vehicle_type=data.type,
        version=data.version,
        odometer=data.odometer,
        language=data.language,
        db=db,
    )

    await send_email(data.language.value == "fr", data.email, token)

    return {"message": f"Mail sent to {data.email}"}


@flash_report_router.get("/generation-data")
async def get_flash_report_data_for_generation(
    request: Request,
    flash_report_user: User | None = Depends(get_flash_report_user),
    token: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    # Get vehicle info from flash report combination
    stmt = select(FlashReportCombination).where(FlashReportCombination.token == token)
    result = await db.execute(stmt)
    flash_report_combination = result.scalar_one_or_none()

    await log_api_call(
        vin=flash_report_combination.vin,
        endpoint=request.scope["route"].path,
        user_id=flash_report_user.id,
        db=db,
    )

    if not flash_report_combination:
        raise HTTPException(
            status_code=404, detail="Cannot get back vehicle info from token.."
        )

    result = await get_flash_report_data(
        flash_report_combination=flash_report_combination, db=db
    )
    return result


@flash_report_router.get("/models-with-trendline", response_model=AllMakesModelsInfo)
async def get_all_models_with_trendline(
    db: AsyncSession = Depends(get_db),
) -> AllMakesModelsInfo:
    query = (
        select(
            Make.make_name,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.version,
        )
        .select_from(VehicleModel)
        .join(Make, VehicleModel.make_id == Make.id)
        .where(VehicleModel.trendline["trendline"].isnot(None))
        .order_by(
            Make.make_name,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.version,
        )
    )
    vehicules_query = await db.execute(query)
    vehicules = vehicules_query.fetchall()

    # Group data hierarchically: makes -> models -> types -> versions
    makes_dict = {}

    for vehicule in vehicules:
        make_name = vehicule.make_name
        model_name = vehicule.model_name
        model_type = vehicule.type
        version = vehicule.version

        # Initialize make if not exists
        if make_name not in makes_dict:
            makes_dict[make_name] = {}

        # Initialize model if not exists
        if model_name not in makes_dict[make_name]:
            makes_dict[make_name][model_name] = {}

        # Initialize type if not exists
        if model_type not in makes_dict[make_name][model_name]:
            makes_dict[make_name][model_name][model_type] = []

        # Add version if not already present
        if version not in makes_dict[make_name][model_name][model_type]:
            makes_dict[make_name][model_name][model_type].append(version)

    # Convert to Pydantic models
    makes = []
    for make_name, models_dict in makes_dict.items():
        models = []
        for model_name, types_dict in models_dict.items():
            types = []
            for model_type, versions in types_dict.items():
                types.append(TypeInfo(model_type=model_type, versions=versions))
            models.append(ModelInfo(model_name=model_name, types=types))
        makes.append(MakeInfo(make_name=make_name, models=models))

    return AllMakesModelsInfo(makes=makes)
