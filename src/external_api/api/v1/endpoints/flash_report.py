from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.db.session import get_db
from external_api.services.flash_report.flash_report import (
    send_email,
    send_flash_report_data,
    send_vehicle_specs,
)

router = APIRouter(prefix="/flash-report", tags=["Flash Report"])


@router.get("/decode/{vin}")
async def decode_vin(
    vin: str = Path(...),
    db: AsyncSession = Depends(get_db),  # noqa: B008
):
    result = await send_vehicle_specs(vin, db)
    if not result:
        raise HTTPException(
            status_code=404, detail="VIN has no been found or can't be decoded"
        )
    return result


@router.post("/send-email")
async def send_report_email(
    email: str = Body(...),
):
    await send_email(email)
    return {"message": f"Mail sent to {email}"}


@router.get("/data")
async def get_flash_report_data(
    make: str = Query(...),
    model: str = Query(...),
    type: str = Query(...),
    odometer: int = Query(...),
    version: str | None = Query(None),
    db: AsyncSession = Depends(get_db),  # noqa: B008
):
    result = await send_flash_report_data(make, model, type, odometer, version, db)
    if not result:
        raise HTTPException(status_code=404, detail="No data found for this vehicle")
    return result

