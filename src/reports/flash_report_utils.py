import logging
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select

from core.s3.async_s3 import AsyncS3
from db_models import (
    Asset,
    FlashReportCombination,
    Report,
    Vehicle,
    VehicleData,
    VehicleModel,
)
from db_models.company import Make, Oem
from db_models.report import ReportType
from db_models.vehicle import Battery
from reports import reports_utils
from reports.report_config import REPORT_S3_BUCKET

logger = logging.getLogger(__name__)


async def fetch_flash_report_required_data(
    frc: FlashReportCombination,
    db: AsyncSession,
) -> tuple[Vehicle, VehicleModel, Battery, Oem, VehicleData, str | None]:
    """
    Fetch or generate all required data for flash report generation.

    Since flash reports are generated before vehicle activation, we need to:
    - Fetch VehicleModel, Battery, Oem, and images from the database
    - Create temporary Vehicle and VehicleData objects (not saved to DB)
    - Calculate SoH values from trendlines

    Args:
        frc: FlashReportCombination with VIN, make, model, type, version, odometer
        db: Database session

    Returns:
        Tuple of (Vehicle, VehicleModel, Battery, Oem, VehicleData, image_url)
    """
    version = frc.version

    # Create aliases for Asset to differentiate make_image and model_image
    MakeImage = aliased(Asset)
    ModelImage = aliased(Asset)

    # If Tesla and no version, get version from vehicle_model
    if frc.make == "tesla" and not version:
        stmt = (
            select(VehicleModel.version)
            .join(Make, Make.id == VehicleModel.make_id)
            .where(
                (Make.make_name == frc.make)
                & (VehicleModel.model_name == frc.model)
                & (VehicleModel.type == frc.type)
                & (
                    VehicleModel.trendline_bib.is_not(None)
                    | VehicleModel.trendline_oem.is_not(None)
                )
            )
        )
        result = await db.execute(stmt)
        result_version = result.fetchone()
        version = result_version[0] if result_version else None

    version = version or None

    # Fetch VehicleModel with Battery, Oem, and Images
    stmt = (
        select(
            VehicleModel,
            Battery,
            Oem,
            ModelImage,
            MakeImage,
        )
        .outerjoin(Make, Make.id == VehicleModel.make_id)
        .outerjoin(Battery, Battery.id == VehicleModel.battery_id)
        .outerjoin(Oem, Oem.id == VehicleModel.oem_id)
        .outerjoin(ModelImage, VehicleModel.image_id == ModelImage.id)
        .outerjoin(MakeImage, Make.image_id == MakeImage.id)
        .where(
            (Make.make_name == frc.make)
            & (VehicleModel.model_name == frc.model)
            & (VehicleModel.type == frc.type)
            & (VehicleModel.version == version)
            & (
                (VehicleModel.trendline_bib.isnot(None))
                | (VehicleModel.trendline_oem.isnot(None))
            )
        )
    )
    result = await db.execute(stmt)
    row = result.first()

    if not row:
        raise ValueError(
            f"Cannot find vehicle model for: make={frc.make}, model={frc.model}, "
            f"type={frc.type}, version={version}"
        )

    vehicle_model, battery, oem, model_asset, make_asset = row

    if not battery:
        raise ValueError(
            f"Battery data not available for: make={frc.make}, model={frc.model}"
        )

    if not oem:
        raise ValueError(
            f"OEM data not available for: make={frc.make}, model={frc.model}"
        )

    image_url = reports_utils.get_image_public_url(model_asset, make_asset)

    # Create temporary Vehicle object (not saved to DB)
    vehicle = Vehicle(
        vin=frc.vin,
        vehicle_model_id=vehicle_model.id,
        licence_plate=None,
        start_date=None,
        bib_score=None,
    )

    vehicle_data = VehicleData(
        odometer=frc.odometer,
    )

    return vehicle, vehicle_model, battery, oem, vehicle_data, image_url


async def generate_flash_report_pdf(token: str, db: AsyncSession) -> UUID:
    """Generate PDF report from FlashReportCombination token."""
    s3_client = AsyncS3()
    try:
        # Fetch FlashReportCombination by token
        frc_query = select(FlashReportCombination).where(
            FlashReportCombination.token == token
        )
        frc_result = await db.execute(frc_query)
        frc = frc_result.scalar_one_or_none()

        if not frc:
            raise ValueError(f"Flash report combination not found for token: {token}")

        vin = frc.vin
        logger.info(f"Generating flash report for VIN: {vin}")

        (
            vehicle,
            vehicle_model,
            battery,
            oem,
            vehicle_data,
            image_url,
        ) = await fetch_flash_report_required_data(frc, db)

        report_uuid, _ = await reports_utils.generate_report_sync(
            vehicle=vehicle,
            vehicle_model=vehicle_model,
            battery=battery,
            oem=oem,
            vehicle_data=vehicle_data,
            image_url=image_url,
            report_type=ReportType.flash,
            db=db,
            s3_client=s3_client,
            s3_bucket=REPORT_S3_BUCKET,
        )

        update_query = (
            update(Report)
            .where(Report.id == report_uuid)
            .values(flash_report_combination_id=frc.id)
        )
        await db.execute(update_query)
        await db.commit()

        return report_uuid
    finally:
        # Properly close the S3 client to avoid unclosed aiohttp session warnings
        await s3_client.close()
