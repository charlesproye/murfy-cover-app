import fastapi
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from db_models.vehicle import FlashReportCombination, User
from external_api.core.utils import get_flash_report_user
from external_api.db.session import get_db
from external_api.schemas.flash_report import (
    FlashReportFormType,
    VehicleSpecs,
)
from external_api.services.api_pricing import log_api_call
from external_api.services.flash_report.flash_report import (
    get_flash_report_data,
    insert_combination,
    send_email,
    send_vehicle_specs,
)

flash_report_router = APIRouter()


@flash_report_router.post("/vin-decoder")
async def decode_vin(
    request: fastapi.Request,
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
    if not result:
        raise HTTPException(
            status_code=404, detail="VIN has no been found or can't be decoded"
        )
    return result


@flash_report_router.post("/send-email")
async def send_report_email(
    request: fastapi.Request,
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
    request: fastapi.Request,
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

