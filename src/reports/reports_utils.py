import logging
from datetime import UTC, date, datetime
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import or_

from core.s3.async_s3 import AsyncS3
from db_models import Asset, Report, Vehicle, VehicleData, VehicleModel
from db_models.company import Make, Oem
from db_models.report import ReportType
from db_models.vehicle import Battery
from external_api.core.config import settings
from reports import reports_utils
from reports.report_config import VERIFY_REPORT_BASE_URL
from reports.report_render.report_generator import ReportGenerator

LOGGER = logging.getLogger(__name__)


def get_image_public_url(
    vehicle_image_asset: Asset | None,
    make_image_asset: Asset | None,
) -> str | None:
    if vehicle_image_asset and vehicle_image_asset.public_url:
        return vehicle_image_asset.public_url
    elif make_image_asset and make_image_asset.public_url:
        return make_image_asset.public_url

    return None


def build_report_data_query(vin: str):
    """
    Build SQLAlchemy query to fetch all required data for report generation.

    This query can be executed with either sync or async sessions.

    Args:
        vin: Vehicle VIN to query

    Returns:
        SQLAlchemy select statement that returns a row with:
        (Vehicle, VehicleModel, Battery, Oem, VehicleData, ModelImage, MakeImage)
    """
    ModelImage = aliased(Asset, name="ModelImage")
    MakeImage = aliased(Asset, name="MakeImage")

    return (
        select(Vehicle, VehicleModel, Battery, Oem, VehicleData, ModelImage, MakeImage)
        .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
        .join(Battery, VehicleModel.battery_id == Battery.id, isouter=True)
        .join(Oem, VehicleModel.oem_id == Oem.id, isouter=True)
        .join(Make, VehicleModel.make_id == Make.id, isouter=True)
        .join(ModelImage, VehicleModel.image_id == ModelImage.id, isouter=True)
        .join(MakeImage, Make.image_id == MakeImage.id, isouter=True)
        .outerjoin(
            VehicleData,
            (VehicleData.vehicle_id == Vehicle.id)
            & (or_(VehicleData.soh_bib.isnot(None), VehicleData.soh_oem.isnot(None))),
        )
        .where(Vehicle.vin == vin)
        .order_by(VehicleData.odometer.desc())
        .limit(1)
    )


async def get_db_report_by_date(
    vin: str,
    report_date: date,
    db: AsyncSession,
    report_type: ReportType,
) -> Report | None:
    """Helper function to get premium report by VIN and date."""
    existing_report_result = await db.execute(
        select(Report)
        .join(Vehicle, Report.vehicle_id == Vehicle.id)
        .where(Vehicle.vin == vin)
        .where(func.date(Report.created_at) == report_date)
        .where(Report.report_type == report_type)
        .limit(1)
    )
    existing_report = existing_report_result.scalar_one_or_none()

    return existing_report


async def get_existing_report_for_today(
    vehicle: Vehicle,
    db: AsyncSession,
    report_type: ReportType,
) -> Report | None:
    """
    Check if a premium report already exists for the given vehicle today.

    Args:
        vehicle: Vehicle DB model
        db: Database session
        report_type: Report type
    Returns:
        PremiumReport if found, None otherwise
    """
    # Use UTC date to match database server timezone (PostgreSQL now() returns UTC)
    today = datetime.now(UTC).date()
    stmt = select(Report).where(
        Report.vehicle_id == vehicle.id,
        func.date(Report.created_at) == today,
        Report.report_type == report_type,
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def save_report_with_race_handling(
    vehicle: Vehicle,
    report_uuid: UUID,
    db: AsyncSession,
    s3_client: AsyncS3,
    s3_uri: str,
    report_type: ReportType,
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
    premium_report = Report(
        vehicle_id=vehicle.id,
        report_url=s3_uri,
        id=report_uuid,
        task_id=None,  # No task ID for sync generation
        report_type=report_type,
    )
    db.add(premium_report)

    try:
        await db.commit()
        LOGGER.info(f"Report saved to database with S3 URI: {s3_uri}")
        return s3_uri
    except IntegrityError:
        # Race condition: another request created a report while we were generating
        await db.rollback()
        LOGGER.warning(
            f"Race condition detected for VIN {vehicle.vin}, fetching existing report"
        )

        existing_report = await get_existing_report_for_today(vehicle, db, report_type)
        if existing_report:
            # Clean up the orphaned S3 file that was just uploaded
            try:
                await s3_client.delete_file(s3_uri=s3_uri)
                LOGGER.info(
                    f"Cleaned up orphaned S3 file due to race condition: {s3_uri}"
                )
            except Exception as e:
                LOGGER.error(
                    f"Failed to clean up orphaned S3 file {s3_uri}: {e}", exc_info=True
                )

            LOGGER.info(
                f"Returning existing report S3 URI from concurrent request: {existing_report.report_url}"
            )
            return existing_report.report_url  # This is the S3 URI
        else:
            # This should never happen, but handle it gracefully
            LOGGER.error(
                f"IntegrityError but no existing report found for VIN {vehicle.vin}"
            )
            raise


async def generate_report_sync(
    vehicle: Vehicle,
    vehicle_model: VehicleModel,
    battery: Battery,
    oem: Oem,
    vehicle_data: VehicleData,
    image_url: str | None,
    db: AsyncSession,
    s3_client: AsyncS3,
    report_type: ReportType,
    s3_bucket: str,
) -> tuple[UUID, str]:
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
        report_type: Report type
        gotenberg_url: URL of the Gotenberg service
        s3_bucket: S3 bucket name for storing PDFs

    Returns:
        S3 URI of the uploaded PDF (stored in database for presigned URL generation)

    Raises:
        ValueError: If required data is missing
        Exception: If PDF generation or upload fails
    """
    vin = vehicle.vin
    LOGGER.info(f"Starting synchronous PDF generation for VIN: {vin}")

    # Only check for existing reports when vehicle has an id (not for flash reports
    # which use temporary Vehicle objects)
    if vehicle.id is not None:
        existing_report = await reports_utils.get_existing_report_for_today(
            vehicle, db, report_type
        )
        if existing_report:
            LOGGER.info(
                f"Report already exists for VIN {vin} today, returning existing S3 URI: {existing_report.report_url}"
            )
            return existing_report.id, existing_report.report_url  # Return S3 URI

    generator = ReportGenerator(gotenberg_url=settings.GOTENBERG_URL)

    report_uuid = uuid4()
    report_data = await generator.generate_report_data(
        vehicle=vehicle,
        vehicle_model=vehicle_model,
        battery=battery,
        oem=oem,
        vehicle_data=vehicle_data,
        image_url=image_url,
        report_uuid=report_uuid,
        report_type=report_type,
        verify_report_base_url=VERIFY_REPORT_BASE_URL,
    )

    html_content = generator.render_template(
        data=report_data,
        embed_assets=True,
    )

    pdf_bytes = await generator.generate_pdf(
        html_content=html_content,
    )

    LOGGER.info(
        f"PDF generated successfully for VIN {vin}, size: {len(pdf_bytes) / 1024:.2f} KB"
    )

    s3_uri = f"s3://{s3_bucket}/{report_type.value}/API/{vin}/{datetime.now(UTC).strftime('%Y%m%d')}_{report_uuid}.pdf"
    await s3_client.upload_file_fast(
        s3_uri=s3_uri,
        file=pdf_bytes,
    )

    s3_uri = await reports_utils.save_report_with_race_handling(
        vehicle=vehicle,
        report_uuid=report_uuid,
        db=db,
        s3_client=s3_client,
        s3_uri=s3_uri,
        report_type=report_type,
    )

    return report_uuid, s3_uri
