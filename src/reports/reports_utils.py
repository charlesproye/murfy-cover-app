import base64
import io
from datetime import date

import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.colormasks import (
    SquareGradiantColorMask,
)
from qrcode.image.styles.moduledrawers.pil import RoundedModuleDrawer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func, or_

from db_models import Asset, Report, Vehicle, VehicleData, VehicleModel
from db_models.company import Make, Oem
from db_models.report import ReportType
from db_models.vehicle import Battery


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


def generate_report_qr_code_data_url(report_url: str) -> str:
    """
    Generate a QR code as a base64 PNG data URL (in-memory, no disk I/O).

    Args:
        report_url: URL of the report

    Returns:
        Base64 data URL string ready for <img src="...">
    """
    qr = qrcode.QRCode(
        version=1,
        box_size=6,
        border=2,
        image_factory=StyledPilImage,
        error_correction=qrcode.ERROR_CORRECT_Q,
    )
    qr.add_data(report_url)
    qr.make(fit=True)

    img = qr.make_image(
        module_drawer=RoundedModuleDrawer(),
        color_mask=SquareGradiantColorMask(
            back_color=(204, 237, 220),
            center_color=(0, 220, 110),
            edge_color=(5, 68, 43),
        ),
    )

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    base64_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{base64_str}"
