import logging
from datetime import UTC, datetime, timedelta

from celery.result import AsyncResult
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from core.s3.async_s3 import AsyncS3
from db_models.report import Report, ReportType
from external_api.core.config import settings
from external_api.schemas.report import (
    ReportData,
    ReportGeneration,
    ReportPDFUrl,
    ReportSync,
)
from external_api.schemas.user import GetCurrentUser
from external_api.services.report import api_report_utils, report_generation
from reports import reports_utils
from reports.exceptions import MissingBIBSoH, MissingOEMSoH
from reports.report_render.report_generator import ReportGenerator
from reports.workers.tasks import generate_pdf_task

LOGGER = logging.getLogger(__name__)


async def get_report_data(
    db: AsyncSession,
    vin: str,
    user: GetCurrentUser,
    report_type: ReportType,
) -> ReportData:
    if not await api_report_utils.check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    # Use the same data source as report generation
    (
        vehicle,
        vehicle_model,
        battery,
        oem,
        vehicle_data,
        image_url,
    ) = await api_report_utils.fetch_report_required_data(vin, db)

    try:
        # Generate report data using the same method as PDF generation
        generator = ReportGenerator()
        return await generator.generate_report_data(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            image_url=image_url,
            report_type=report_type,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        LOGGER.error(f"Error generating data for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Data generation failed: {e!s}"
        ) from e


async def submit_report_job(
    db: AsyncSession,
    vin: str,
    user: GetCurrentUser,
    report_type: ReportType,
) -> ReportGeneration:
    if not await api_report_utils.check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    today = datetime.now(UTC).date()
    existing_report = await reports_utils.get_db_report_by_date(
        vin, today, db, report_type
    )
    if existing_report:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Report already exists for today",
                "report_id": str(existing_report.id),
                "vin": vin,
                "date": today.isoformat(),
            },
        )

    if not await api_report_utils.check_soh_available(vin, db, report_type):
        raise HTTPException(
            status_code=400, detail="Vehicle activated but SoH is not available yet."
        )

    task = generate_pdf_task.delay(vin, report_type)

    return ReportGeneration(
        job_id=task.id, message="PDF generation started", estimated_duration="2 minutes"
    )


async def get_report_job_status(
    vin: str,
    job_id: str,
    user: GetCurrentUser,
    db: AsyncSession,
    s3_client: AsyncS3,
) -> ReportPDFUrl:
    if not await api_report_utils.check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    task_result = AsyncResult(job_id)

    premium_report_result = await db.execute(
        select(Report).where(Report.task_id == job_id)
    )
    premium_report = premium_report_result.scalar_one_or_none()

    message = "Processing"
    url: str | None = None
    error: str | None = None
    retry_info: str | None = None

    if task_result.state == "PENDING":
        message = "Task is waiting to be processed"

    elif task_result.state == "STARTED":
        message = "PDF generation in progress"

    elif task_result.state == "SUCCESS":
        message = "PDF generated successfully"
        if premium_report and premium_report.report_url:
            url = await s3_client.get_presigned_url(
                s3_uri=premium_report.report_url,
                expires_in=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN,
            )

    elif task_result.state == "FAILURE":
        message = "PDF generation failed"
        error = str(task_result.info)

    elif task_result.state == "RETRY":
        message = "PDF generation failed, retrying..."
        retry_info = str(task_result.info)
    else:
        if premium_report and premium_report.report_url:
            message = "PDF generated successfully"
            url = await s3_client.get_presigned_url(
                s3_uri=premium_report.report_url,
                expires_in=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN,
            )
        else:
            message = f"Unknown state: {task_result.state}"

    expires_at = (
        datetime.now(UTC)
        + timedelta(seconds=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN)
        if url
        else None
    )

    return ReportPDFUrl(
        job_id=job_id,
        vin=vin,
        message=message,
        url=url,
        expires_at=expires_at,
        error=error,
        retry_info=retry_info,
    )


async def get_report_for_date(
    vin: str,
    iso_date: str,
    db: AsyncSession,
    s3_client: AsyncS3,
    user: GetCurrentUser,
    report_type: ReportType,
) -> ReportPDFUrl:
    if not await api_report_utils.check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    try:
        report_date = datetime.strptime(iso_date, "%Y-%m-%d").date()
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {iso_date}. Expected format: YYYY-MM-DD",
        ) from e

    return await report_generation.get_report_by_date(
        vin, report_date, db, s3_client, report_type=report_type
    )


async def generate_html_report(
    vin: str,
    db: AsyncSession,
    user: GetCurrentUser,
    report_type: ReportType,
) -> HTMLResponse:
    if not await api_report_utils.check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    (
        vehicle,
        vehicle_model,
        battery,
        oem,
        vehicle_data,
        image_url,
    ) = await api_report_utils.fetch_report_required_data(vin, db)

    try:
        generator = ReportGenerator()
        html_content = await generator.generate_report_html(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            image_url=image_url,
            report_type=report_type,
        )
        return HTMLResponse(content=html_content)
    except (MissingOEMSoH, MissingBIBSoH) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        LOGGER.error(f"Error generating HTML for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"HTML generation failed: {e!s}"
        ) from e


async def genereate_pdf_report_sync(
    vin: str,
    db: AsyncSession,
    s3_client: AsyncS3,
    user: GetCurrentUser,
    report_type: ReportType,
) -> ReportSync:
    if not await api_report_utils.check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    (
        vehicle,
        vehicle_model,
        battery,
        oem,
        vehicle_data,
        image_url,
    ) = await api_report_utils.fetch_report_required_data(vin, db)

    try:
        s3_uri = await report_generation.generate_report_sync(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            db=db,
            s3_client=s3_client,
            image_url=image_url,
            report_type=report_type,
        )

        presigned_url = await s3_client.get_presigned_url(
            s3_uri=s3_uri,
            expires_in=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN,
        )

        return ReportSync(
            vin=vin,
            url=presigned_url,
            message="PDF generated and uploaded successfully",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        LOGGER.error(f"Error generating PDF for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"PDF generation failed: {e!s}"
        ) from e
