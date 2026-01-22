from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from core.sql_utils import get_async_db
from db_models import Make, User, VehicleModel
from external_api.schemas.flash_report import FlashReportFormType, VehicleSpecs
from external_api.schemas.static_data import (
    AllMakesModelsInfo,
    MakeInfo,
    ModelInfo,
    TypeInfo,
)
from external_api.services.api_pricing import log_api_call
from external_api.services.flash_report.flash_report import (
    insert_combination,
    log_vin_decoded,
    send_vehicle_specs,
)
from external_api.services.user import get_flash_report_user
from reports.workers.flash_report_task import generate_pdf_send_email_task

flash_report_router = APIRouter()


@flash_report_router.post("/vin-decoder")
async def decode_vin(
    request: Request,
    vin: str = Body(..., description="VIN to decode"),
    flash_report_user: User = Depends(get_flash_report_user),
    db: AsyncSession = Depends(get_async_db),
) -> VehicleSpecs:
    await log_api_call(
        vin=vin,
        endpoint=request.scope["route"].path,
        user_id=flash_report_user.id,
        db=db,
    )

    result = await send_vehicle_specs(vin, db)

    if (
        result.make
        and not result.has_trendline_oem
        and not result.has_trendline_bib
        and result.model
    ):
        await log_vin_decoded(db, vin, result.make, result.model)
    return result


@flash_report_router.post("/send-report-generation")
async def send_report_generation(
    request: Request,
    data: FlashReportFormType = Body(...),
    db: AsyncSession = Depends(get_async_db),
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

    generate_pdf_send_email_task.delay(token, data.email, data.language)

    return {"message": f"Mail sent to {data.email}"}


@flash_report_router.get("/models-with-trendline", response_model=AllMakesModelsInfo)
async def get_all_models_with_trendline(
    db: AsyncSession = Depends(get_async_db),
) -> AllMakesModelsInfo:
    query = (
        select(
            Make.make_name,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.version,
            VehicleModel.has_trendline_bib,
            VehicleModel.has_trendline_oem,
        )
        .select_from(VehicleModel)
        .join(Make, VehicleModel.make_id == Make.id)
        .where(
            (VehicleModel.trendline_bib.isnot(None))
            | (VehicleModel.trendline_oem.isnot(None))
        )
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
        has_trendline_bib = vehicule.has_trendline_bib
        has_trendline_oem = vehicule.has_trendline_oem

        # Initialize make if not exists
        if make_name not in makes_dict:
            makes_dict[make_name] = {}

        # Initialize model if not exists
        if model_name not in makes_dict[make_name]:
            makes_dict[make_name][model_name] = {}

        # Initialize type if not exists
        if model_type not in makes_dict[make_name][model_name]:
            makes_dict[make_name][model_name][model_type] = {
                "versions": [],
                "has_trendline_bib": has_trendline_bib,
                "has_trendline_oem": has_trendline_oem,
            }

        # Add version if not already present
        # Check if the value is None (meaning no versions) before using 'in' operator
        type_info = makes_dict[make_name][model_name][model_type]
        versions_list = type_info["versions"]
        if versions_list is None:
            # Already set to None (no versions), skip
            continue

        if version not in versions_list:
            if version:
                versions_list.append(version)
            else:
                # Set to None to indicate no versions for this type
                type_info["versions"] = None

    # Convert to Pydantic models
    makes = []
    for make_name, models_dict in makes_dict.items():
        models = []
        for model_name, types_dict in models_dict.items():
            types = []
            for model_type, type_info in types_dict.items():
                if model_type:
                    types.append(
                        TypeInfo(
                            model_type=model_type,
                            versions=type_info["versions"],
                            has_trendline_bib=type_info["has_trendline_bib"],
                            has_trendline_oem=type_info["has_trendline_oem"],
                        )
                    )
            models.append(ModelInfo(model_name=model_name, types=types))
        makes.append(MakeInfo(make_name=make_name, models=models))

    return AllMakesModelsInfo(makes=makes)
