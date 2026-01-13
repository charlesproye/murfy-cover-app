"""Service for generating premium reports synchronously."""

import logging
import uuid
from datetime import UTC, date, datetime, timedelta

from fastapi.exceptions import HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.s3.async_s3 import AsyncS3
from db_models.company import Oem
from db_models.report import PremiumReport
from db_models.vehicle import (
    Battery,
    Vehicle,
    VehicleData,
    VehicleModel,
)
from external_api.core.config import settings
from external_api.schemas.premium import PremiumReportPDFUrl
from reports import reports_utils
from reports.report_render.premium_report_generator import PremiumReportGenerator

logger = logging.getLogger(__name__)


async def get_existing_report_for_today(
    vehicle: Vehicle, db: AsyncSession
) -> PremiumReport | None:
    """
    Check if a premium report already exists for the given vehicle today.

    Args:
        vehicle: Vehicle DB model
        db: Database session

    Returns:
        PremiumReport if found, None otherwise
    """
    # Use UTC date to match database server timezone (PostgreSQL now() returns UTC)
    today = datetime.now(UTC).date()
    stmt = select(PremiumReport).where(
        PremiumReport.vehicle_id == vehicle.id,
        func.date(PremiumReport.created_at) == today,
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def _save_report_with_race_handling(
    vehicle: Vehicle,
    report_uuid: str,
    db: AsyncSession,
    s3_client: AsyncS3,
    s3_uri: str,
) -> str:
    """
    Save premium report to database with race condition handling.

    Attempts to insert the report. If a race condition occurs (IntegrityError due to
    unique constraint violation), fetches and returns the existing report instead.
    When a race occurs, cleans up the orphaned S3 file to prevent storage waste.

    Args:
        vehicle: Vehicle DB model
        report_uuid: UUID for the report
        db: Database session
        s3_client: AsyncS3 client for cleanup operations
        s3_uri: S3 path of the uploaded file (for cleanup on race condition)

    Returns:
        S3 URI of the saved report (either newly created or existing from concurrent request)

    Raises:
        IntegrityError: If an unexpected integrity error occurs
    """
    premium_report = PremiumReport(
        vehicle_id=vehicle.id,
        report_url=s3_uri,
        id=uuid.UUID(report_uuid),
        task_id=None,  # No task ID for sync generation
    )
    db.add(premium_report)

    try:
        await db.commit()
        logger.info(f"Report saved to database with S3 URI: {s3_uri}")
        return s3_uri
    except IntegrityError:
        # Race condition: another request created a report while we were generating
        await db.rollback()
        logger.warning(
            f"Race condition detected for VIN {vehicle.vin}, fetching existing report"
        )

        existing_report = await get_existing_report_for_today(vehicle, db)
        if existing_report:
            # Clean up the orphaned S3 file that was just uploaded
            try:
                await s3_client.delete_file(s3_uri=s3_uri)
                logger.info(
                    f"Cleaned up orphaned S3 file due to race condition: {s3_uri}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to clean up orphaned S3 file {s3_uri}: {e}", exc_info=True
                )

            logger.info(
                f"Returning existing report S3 URI from concurrent request: {existing_report.report_url}"
            )
            return existing_report.report_url  # This is the S3 URI
        else:
            # This should never happen, but handle it gracefully
            logger.error(
                f"IntegrityError but no existing report found for VIN {vehicle.vin}"
            )
            raise


async def generate_premium_report_sync(
    vehicle: Vehicle,
    vehicle_model: VehicleModel,
    battery: Battery,
    oem: Oem,
    vehicle_data: VehicleData,
    image_url: str | None,
    db: AsyncSession,
    s3_client: AsyncS3,
    gotenberg_url: str = settings.GOTENBERG_URL,
    s3_bucket: str = settings.PREMIUM_REPORT_S3_BUCKET,
) -> str:
    """
    Generate a premium PDF report synchronously and upload to S3.

    Args:
        vehicle: Vehicle DB model
        vehicle_model: VehicleModel DB model
        battery: Battery DB model
        oem: Oem DB model
        vehicle_data: Latest VehicleData DB model
        db: Database session
        s3_client: AsyncS3 client instance (injected dependency)
        gotenberg_url: URL of the Gotenberg service
        s3_bucket: S3 bucket name for storing PDFs

    Returns:
        S3 URI of the uploaded PDF (stored in database for presigned URL generation)

    Raises:
        ValueError: If required data is missing
        Exception: If PDF generation or upload fails
    """
    vin = vehicle.vin
    logger.info(f"Starting synchronous PDF generation for VIN: {vin}")

    existing_report = await get_existing_report_for_today(vehicle, db)
    if existing_report:
        logger.info(
            f"Report already exists for VIN {vin} today, returning existing S3 URI: {existing_report.report_url}"
        )
        return existing_report.report_url  # Return S3 URI

    generator = PremiumReportGenerator(gotenberg_url=gotenberg_url)

    report_uuid = str(uuid.uuid4())
    report_data = await generator.generate_premium_report_data(
        vehicle=vehicle,
        vehicle_model=vehicle_model,
        battery=battery,
        oem=oem,
        vehicle_data=vehicle_data,
        image_url=image_url,
        report_uuid=report_uuid,
    )

    html_content = generator.render_template(
        data=report_data,
        embed_assets=True,
    )

    pdf_bytes = await generator.generate_pdf(
        html_content=html_content,
    )

    logger.info(
        f"PDF generated successfully for VIN {vin}, size: {len(pdf_bytes) / 1024:.2f} KB"
    )

    s3_uri = f"s3://{s3_bucket}/sync/{vin}/{datetime.now(UTC).strftime('%Y%m%d')}_{uuid.uuid4()}.pdf"
    await s3_client.upload_file_fast(
        s3_uri=s3_uri,
        file=pdf_bytes,
    )

    return await _save_report_with_race_handling(
        vehicle=vehicle,
        report_uuid=report_uuid,
        db=db,
        s3_client=s3_client,
        s3_uri=s3_uri,
    )


async def get_premium_report_by_date(
    vin: str,
    report_date: date,
    db: AsyncSession,
    s3_client: AsyncS3,
) -> PremiumReportPDFUrl:
    existing_report = await reports_utils.get_db_premium_report_by_date(
        vin, report_date, db
    )
    if not existing_report:
        raise HTTPException(
            status_code=404,
            detail=f"Report not found for VIN={vin} and date={report_date.isoformat()}",
        )

    url = await s3_client.get_presigned_url(
        s3_uri=existing_report.report_url,
        expires_in=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN,
    )

    expires_at = datetime.now(UTC) + timedelta(
        seconds=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN
    )

    return PremiumReportPDFUrl(
        vin=vin,
        url=url,
        expires_at=expires_at,
    )
