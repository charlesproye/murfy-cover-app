from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db_models.vehicle import Fleet, PremiumReport, UserFleet, Vehicle, VehicleData
from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleet,
)
from external_api.db.session import get_db
from external_api.schemas.passport import PassportCrud
from external_api.schemas.premium_report import PremiumReportData, PremiumReportPDFUrl
from external_api.schemas.user import GetCurrentUser
from reports.workers.tasks import generate_pdf_task

router = APIRouter()


async def check_user_allowed_to_vin(
    vin: str, user: GetCurrentUser, db: AsyncSession
) -> bool:
    stmt = (
        select(UserFleet.user_id)
        .distinct()
        .select_from(Vehicle)
        .join(Fleet, Vehicle.fleet_id == Fleet.id, isouter=True)
        .join(UserFleet, UserFleet.fleet_id == Fleet.id, isouter=True)
        .where(Vehicle.vin == vin)
    )

    result = await db.execute(stmt)
    allowed_users = result.scalars().all()

    return user.id in allowed_users


async def check_vehicle_activated(vin: str, db: AsyncSession) -> bool:
    stmt = (
        select(Vehicle.activation_status).select_from(Vehicle).where(Vehicle.vin == vin)
    )
    result = await db.execute(stmt)
    return result.scalar()


async def check_soh_available(vin: str, db: AsyncSession) -> bool:
    stmt = (
        select(VehicleData.soh)
        .select_from(VehicleData)
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(Vehicle.vin == vin)
    )

    result = await db.execute(stmt)
    sohs = result.scalars().all()
    return any(soh is not None for soh in sohs)


@router.post("/{vin}/generate", tags=["Reports"], status_code=202)
async def generate_premium_report(
    db=Depends(get_db),
    vin: str = Path(..., description="VIN requested"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
) -> PremiumReportData:
    if not await check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    if not await check_vehicle_activated(vin, db):
        raise HTTPException(
            status_code=400, detail="Selected vehicle is not activated."
        )

    if not await check_soh_available(vin, db):
        raise HTTPException(
            status_code=400, detail="Vehicle activated but SoH is not available yet."
        )

    response = await PassportCrud().get_infos(vin, db)

    response_data = response if response is not None else {}

    task = generate_pdf_task.delay(vin)

    response_data["report"] = {
        "job_id": task.id,
        "message": "PDF generation started",
        "estimated_duration": "2 minutes",
    }

    return PremiumReportData(**response_data)


@router.get("/{vin}/status/{job_id}", tags=["Reports"])
async def get_report_status(
    vin: str = Path(..., description="The VIN"),
    job_id: str = Path(..., description="The Celery job ID"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleet)),
    db: AsyncSession = Depends(get_db),
) -> PremiumReportPDFUrl:
    if not await check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    task_result = AsyncResult(job_id)

    premium_report_result = await db.execute(
        select(PremiumReport).where(PremiumReport.task_id == job_id)
    )
    premium_report = premium_report_result.scalar_one_or_none()

    response = {
        "job_id": job_id,
        "vin": vin,
        "url": None,
        "error": None,
        "retry_info": None,
    }

    if task_result.state == "PENDING":
        response["message"] = "Task is waiting to be processed"

    elif task_result.state == "STARTED":
        response["message"] = "PDF generation in progress"

    elif task_result.state == "SUCCESS":
        response["message"] = "PDF generated successfully"
        if premium_report and premium_report.report_url:
            response["url"] = premium_report.report_url

    elif task_result.state == "FAILURE":
        response["message"] = "PDF generation failed"
        response["error"] = str(task_result.info)

    elif task_result.state == "RETRY":
        response["message"] = "PDF generation failed, retrying..."
        response["retry_info"] = str(task_result.info)
    else:
        if premium_report and premium_report.report_url:
            response["message"] = "PDF generated successfully"
            response["url"] = premium_report.report_url
        else:
            response["message"] = f"Unknown state: {task_result.state}"

    return PremiumReportPDFUrl(**response)
