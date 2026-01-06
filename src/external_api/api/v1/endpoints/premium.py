import logging

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from core.s3.async_s3 import AsyncS3
from db_models import Asset, Fleet, PremiumReport, UserFleet, Vehicle, VehicleData
from db_models.company import Make, Oem
from db_models.vehicle import (
    Battery,
    VehicleModel,
)
from external_api.core.config import settings
from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleets,
)
from external_api.core.dependencies import get_s3_client_fast
from external_api.db.session import get_db
from external_api.schemas.premium import (
    PremiumReportData,
    PremiumReportGeneration,
    PremiumReportPDFUrl,
    PremiumReportSync,
)
from external_api.schemas.user import GetCurrentUser
from external_api.services.premium.report_generation import (
    generate_premium_report_sync,
)
from reports import reports_utils
from reports.report_render.premium_report_generator import PremiumReportGenerator
from reports.workers.tasks import generate_pdf_task

router = APIRouter()
logger = logging.getLogger(__name__)


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


async def check_soh_available(vin: str, db: AsyncSession) -> bool:
    stmt = (
        select(VehicleData.id)
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(Vehicle.vin == vin)
        .where(VehicleData.soh.isnot(None))
        .limit(1)
    )

    result = await db.execute(stmt)
    return result.scalar_one_or_none() is not None


@router.get(
    "/{vin}/data",
    response_model=PremiumReportData,
    summary="Get data by vehicle",
    description="Returns the complete premium report data collected by Bib for an activated vehicle.",
)
async def get_premium_data(
    db=Depends(get_db),
    vin: str = Path(..., description="VIN requested"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> PremiumReportData:
    if not await check_user_allowed_to_vin(vin, user, db):
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
    ) = await fetch_report_required_data(vin, db)

    try:
        # Generate report data using the same method as PDF generation
        generator = PremiumReportGenerator()
        return await generator.generate_premium_report_data(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            image_url=image_url,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error generating data for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Data generation failed: {e!s}"
        ) from e


@router.post(
    "/{vin}/generate_report",
    status_code=202,
    summary="Generate premium report",
    description="Triggers the generation of a premium report for a vehicle by VIN. The report will only be generated if the vehicle is activated and has SoH data available, it can take up to 2 minutes to generate the report. Use the `Get premium report` endpoint to retrieve the status and URL of the report.",
)
async def generate_premium_report(
    db=Depends(get_db),
    vin: str = Path(..., description="VIN requested"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> PremiumReportGeneration:
    if not await check_user_allowed_to_vin(vin, user, db):
        raise HTTPException(
            status_code=422, detail="User not allowed to access this VIN"
        )

    if not await check_soh_available(vin, db):
        raise HTTPException(
            status_code=400, detail="Vehicle activated but SoH is not available yet."
        )

    task = generate_pdf_task.delay(vin)

    response_data = {
        "job_id": task.id,
        "message": "PDF generation started",
        "estimated_duration": "2 minutes",
    }

    return PremiumReportGeneration(**response_data)


@router.get(
    "/{vin}/report/{job_id}",
    summary="Get premium report",
    description="Returns the status and URL of a premium report for a vehicle using the job_id returned by the `Generate premium report` endpoint.",
)
async def get_report_status(
    vin: str = Path(..., description="The VIN"),
    job_id: str = Path(..., description="The Celery job ID"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
    db: AsyncSession = Depends(get_db),
    s3_client: AsyncS3 = Depends(get_s3_client_fast),
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

    return PremiumReportPDFUrl(
        job_id=job_id,
        vin=vin,
        message=message,
        url=url,
        error=error,
        retry_info=retry_info,
    )


async def fetch_report_required_data(
    vin: str, db: AsyncSession
) -> tuple[Vehicle, VehicleModel, Battery, Oem, VehicleData, str | None]:
    """Fetch all required data for report generation in a single query."""

    ModelImage = aliased(Asset, name="ModelImage")
    MakeImage = aliased(Asset, name="MakeImage")

    stmt = (
        select(Vehicle, VehicleModel, Battery, Oem, VehicleData, ModelImage, MakeImage)
        .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
        .join(Battery, VehicleModel.battery_id == Battery.id, isouter=True)
        .join(Oem, VehicleModel.oem_id == Oem.id, isouter=True)
        .join(Make, VehicleModel.make_id == Make.id, isouter=True)
        .join(ModelImage, VehicleModel.image_id == ModelImage.id, isouter=True)
        .join(MakeImage, Make.image_id == MakeImage.id, isouter=True)
        .outerjoin(
            VehicleData,
            (VehicleData.vehicle_id == Vehicle.id) & (VehicleData.soh.isnot(None)),
        )
        .where(Vehicle.vin == vin)
        .order_by(
            VehicleData.odometer.desc()
        )  # ID/created/updated at are not reliable for sorting
        .limit(1)
    )

    result = await db.execute(stmt)
    row = result.first()

    if not row:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    # Validate required relationships exist
    if row.Battery is None:
        raise HTTPException(
            status_code=400,
            detail="Battery data not available for this vehicle model",
        )

    if row.Oem is None:
        raise HTTPException(
            status_code=400, detail="OEM data not available for this vehicle model"
        )

    if row.VehicleData is None or row.VehicleData.soh is None:
        raise HTTPException(
            status_code=400, detail="Vehicle activated but SoH is not available yet."
        )

    image_url = reports_utils.get_image_url(row.ModelImage, row.MakeImage)

    return (
        row.Vehicle,
        row.VehicleModel,
        row.Battery,
        row.Oem,
        row.VehicleData,
        image_url,
    )


@router.get(
    "/{vin}/report_html",
    response_class=HTMLResponse,
    summary="Get premium report HTML",
    description="Generates the HTML content for a premium report by VIN.",
)
async def get_premium_report_html_endpoint(
    vin: str = Path(..., description="VIN requested"),
    db: AsyncSession = Depends(get_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> HTMLResponse:
    if not await check_user_allowed_to_vin(vin, user, db):
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
    ) = await fetch_report_required_data(vin, db)

    try:
        generator = PremiumReportGenerator()
        html_content = await generator.generate_premium_report_html(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            image_url=image_url,
        )
        return HTMLResponse(content=html_content)
    except Exception as e:
        logger.error(f"Error generating HTML for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"HTML generation failed: {e!s}"
        ) from e


@router.post(
    "/{vin}/generate_report_sync",
    response_model=PremiumReportSync,
    summary="Generate premium report synchronously",
    description="Generates a premium report for a vehicle by VIN synchronously. The report will be generated immediately and uploaded to S3. This endpoint may take up to 30 seconds to respond.",
)
async def generate_premium_report_sync_endpoint(
    vin: str = Path(..., description="VIN requested"),
    db: AsyncSession = Depends(get_db),
    s3_client: AsyncS3 = Depends(get_s3_client_fast),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> PremiumReportSync:
    if not await check_user_allowed_to_vin(vin, user, db):
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
    ) = await fetch_report_required_data(vin, db)

    try:
        s3_uri = await generate_premium_report_sync(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            db=db,
            s3_client=s3_client,
            image_url=image_url,
        )

        presigned_url = await s3_client.get_presigned_url(
            s3_uri=s3_uri,
            expires_in=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN,
        )

        return PremiumReportSync(
            vin=vin,
            url=presigned_url,
            message="PDF generated and uploaded successfully",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error generating PDF for VIN {vin}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"PDF generation failed: {e!s}"
        ) from e
