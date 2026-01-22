import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.sql_utils import get_async_db
from db_models import Make
from db_models.fleet import Fleet, UserFleet
from db_models.vehicle import Vehicle, VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.core.utils import verify_user_fleet_access
from external_api.schemas.user import GetCurrentUser
from external_api.schemas.vehicle_command import (
    FleetInfo,
    FleetInfoResponse,
    VehicleStatus,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/fleets",
    response_model=FleetInfoResponse,
    summary="Get the fleets of the account",
    description="Get the fleet(s) owned by the account",
)
async def get_fleets(
    db: AsyncSession = Depends(get_async_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
) -> FleetInfoResponse:
    fleets = await db.execute(
        select(Fleet).join(UserFleet).where(UserFleet.user_id == user.id)
    )
    fleets = fleets.scalars().all()
    return FleetInfoResponse(
        fleets=[
            FleetInfo(fleet_id=str(fleet.id), fleet_name=fleet.fleet_name)
            for fleet in fleets
        ]
    )


@router.get(
    "/vehicles",
    response_model=list[VehicleStatus],
    summary="Get the status of all vehicles",
    description="Get the status of all vehicles",
)
async def get_vehicle_status(
    fleet_id: str = Query(..., description="Fleet ID"),
    vin: str = Query(None, description="VIN of the vehicle"),
    db: AsyncSession = Depends(get_async_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
) -> list[VehicleStatus]:
    # Verify user has access to the fleet
    has_access = await verify_user_fleet_access(db, user.id, uuid.UUID(fleet_id))
    if not has_access:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User does not have access to fleet {fleet_id}",
        )

    if vin:
        vehicles_query = (
            select(Vehicle, VehicleModel, Make)
            .outerjoin(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
            .outerjoin(Make, VehicleModel.make_id == Make.id)
            .where(Vehicle.fleet_id == fleet_id, Vehicle.vin == vin)
        )
    else:
        vehicles_query = (
            select(Vehicle, VehicleModel, Make)
            .outerjoin(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
            .outerjoin(Make, VehicleModel.make_id == Make.id)
            .where(Vehicle.fleet_id == fleet_id)
        )

    result = await db.execute(vehicles_query)
    vehicles_data = result.all()

    return [
        VehicleStatus(
            vin=vehicle.vin or "",
            requested_soh_readout=vehicle.readout_report_requested_status or False,
            requested_soh_bib=vehicle.bib_report_requested_status or False,
            requested_activation=vehicle.activation_requested_status or False,
            status=vehicle.activation_status,
            message=vehicle.activation_status_message,
            comment=vehicle.activation_comment,
        )
        for vehicle, model, make in vehicles_data
    ]
