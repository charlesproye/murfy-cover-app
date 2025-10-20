from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.db.session import get_db
from external_api.schemas.flash_report import FlashReportFormType
from external_api.services.flash_report.flash_report import (
    insert_combination,
    send_email,
    send_flash_report_data,
    send_vehicle_specs,
)

router = APIRouter()


@router.post("/vin-decoder")
async def decode_vin(
    vin: str = Body(..., description="VIN to decode"),
    db: AsyncSession = Depends(get_db),
):
    result = await send_vehicle_specs(vin, db)
    if not result:
        raise HTTPException(
            status_code=404, detail="VIN has no been found or can't be decoded"
        )
    return result


@router.post("/send-email")
async def send_report_email(
    data: FlashReportFormType = Body(...),
    db: AsyncSession = Depends(get_db),
):
    token = await insert_combination(
        make=data.make,
        model=data.model,
        type=data.type,
        version=data.version,
        odometer=data.odometer,
        db=db,
    )

    await send_email(data.language.value == "fr", data.email, token)

    return {"message": f"Mail sent to {data.email}"}


@router.get("/data")
async def get_flash_report_data(
    make: str = Query(...),
    model: str = Query(...),
    type: str = Query(...),
    odometer: int = Query(...),
    version: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    result = await send_flash_report_data(make, model, type, odometer, version, db)
    if not result:
        raise HTTPException(status_code=404, detail="No data found for this vehicle")
    return result

