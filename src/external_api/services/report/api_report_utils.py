from fastapi.exceptions import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Battery, Fleet, Oem, UserFleet, Vehicle, VehicleData, VehicleModel
from db_models.report import ReportType
from external_api.schemas.user import GetCurrentUser
from reports import reports_utils


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


async def check_soh_available(
    vin: str, db: AsyncSession, report_type: ReportType
) -> bool:
    if report_type == ReportType.premium:
        condition = VehicleData.soh.isnot(None)
    elif report_type == ReportType.readout:
        condition = VehicleData.soh_oem.isnot(None)
    else:
        raise ValueError(f"Invalid report type: {report_type}")

    stmt = (
        select(VehicleData.id)
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(Vehicle.vin == vin)
        .where(condition)
        .limit(1)
    )

    result = await db.execute(stmt)
    return result.scalar_one_or_none() is not None


async def fetch_report_required_data(
    vin: str, db: AsyncSession
) -> tuple[Vehicle, VehicleModel, Battery, Oem, VehicleData, str | None]:
    """Fetch all required data for report generation in a single query."""

    stmt = reports_utils.build_report_data_query(vin)

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

    vehichle_data: VehicleData = row.VehicleData
    if vehichle_data is None or (
        vehichle_data.soh is None and vehichle_data.soh_oem is None
    ):
        raise HTTPException(
            status_code=400, detail="Vehicle activated but SoH is not available yet."
        )

    image_url = reports_utils.get_image_public_url(row.ModelImage, row.MakeImage)

    return (
        row.Vehicle,
        row.VehicleModel,
        row.Battery,
        row.Oem,
        row.VehicleData,
        image_url,
    )
