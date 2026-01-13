from datetime import date

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
            & (or_(VehicleData.soh.isnot(None), VehicleData.soh_oem.isnot(None))),
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
