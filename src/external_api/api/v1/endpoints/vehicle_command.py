import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.sql_utils import get_async_db
from db_models import Make
from db_models.vehicle import Battery, Vehicle, VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.core.utils import verify_user_fleet_access
from external_api.schemas.user import GetCurrentUser
from external_api.schemas.vehicle_command import (
    ActivationRequest,
    ActivationResponse,
    DeactivationRequest,
    DeactivationResponse,
    MakeInfoResponse,
    ModelInfo,
    ModelInfoResponse,
    ModelType,
    VehicleStatus,
)
from external_api.services.vehicle_command import (
    _get_available_makes,
    _insert_vehicle_activation_history,
    calculate_activation_dates,
    process_vehicle_activation,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/makes",
    response_model=MakeInfoResponse,
    summary="Get all available vehicle makes",
    description="Get the list of all vehicle makes available in the database. Use these makes when activating vehicles.",
    include_in_schema=False,
)
async def get_makes(
    db: AsyncSession = Depends(get_async_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
) -> MakeInfoResponse:
    """
    Retrieve all available vehicle makes from the database.
    This endpoint should be used to get the valid makes before activating vehicles.
    """
    available_makes = await _get_available_makes(db)
    return MakeInfoResponse(makes=available_makes)


@router.get(
    "/{make_id}/models",
    response_model=ModelInfoResponse,
    summary="Get all models for a specific make",
    description="Get the list of all vehicle models available for a specific make. Use these models when activating vehicles.",
    include_in_schema=False,
)
async def get_models_for_make(
    make_id: str,
    db: AsyncSession = Depends(get_async_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
) -> ModelInfoResponse:
    """
    Retrieve all available vehicle models for a specific make from the database.
    This endpoint should be used to get the valid models for a make before activating vehicles.
    """
    # Query all vehicle models with joins for make and battery info
    query = (
        select(
            VehicleModel.id,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.version,
            Battery.capacity,
        )
        .join(Make, VehicleModel.make_id == Make.id)
        .outerjoin(Battery, VehicleModel.battery_id == Battery.id)
        .where(Make.id == uuid.UUID(make_id))
        .order_by(VehicleModel.model_name, VehicleModel.type)
    )

    result = await db.execute(query)
    rows = result.all()

    models = [
        ModelInfo(
            model_id=str(row.id),
            model_name=row.model_name,
            type=row.type,
            version=row.version,
            battery_capacity=float(row.capacity) if row.capacity else None,
        )
        for row in rows
    ]
    return ModelInfoResponse(models=models)


@router.get(
    "/eligible-models",
    response_model=list[ModelType],
    summary="Get all models eligible",
    description="Get the list of all vehicle models eligibile in the database. Use these models when activating vehicles.",
)
async def get_models_eligibile(
    has_flash_soh: bool | None = Query(None),
    has_oem_soh: bool | None = Query(None),
    has_bib_soh: bool | None = Query(None),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
) -> list[ModelType]:
    query = (
        select(
            VehicleModel.id,
            Make.make_name,
            VehicleModel.model_name,
            VehicleModel.type,
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            VehicleModel.trendline_bib,
            VehicleModel.trendline_oem,
            VehicleModel.soh_data,
            VehicleModel.soh_oem_data,
        )
        .select_from(VehicleModel)
        .join(Make, VehicleModel.make_id == Make.id)
        .join(Battery, VehicleModel.battery_id == Battery.id)
    )

    if has_flash_soh:
        query = query.where(
            (VehicleModel.trendline_bib.is_not(None))
            | (VehicleModel.trendline_oem.is_not(None))
        )
    if has_oem_soh:
        query = query.where(VehicleModel.soh_oem_data.is_(True))
    if has_bib_soh:
        query = query.where(VehicleModel.soh_data.is_(True))

    vehicules_query = await db.execute(query)
    vehicules = vehicules_query.fetchall()

    return [
        ModelType(
            model_id=vehicule.id,
            make=vehicule.make_name,
            model_name=vehicule.model_name,
            model_type=vehicule.type,
            commissioning_date=vehicule.commissioning_date,
            end_of_life_date=vehicule.end_of_life_date,
            has_flash_soh=(vehicule.trendline_bib is not None)
            or (vehicule.trendline_oem is not None),
            has_oem_soh=vehicule.soh_oem_data,
            has_bib_soh=vehicule.soh_data,
        )
        for vehicule in vehicules
    ]


@router.post(
    "/order-soh-report",
    response_model=ActivationResponse,
    summary="Activate vehicles with automatic date management",
    description="Activate vehicles with dates calculated automatically based on SoH type requested.",
)
async def activate_vehicles_auto(
    activation: ActivationRequest,
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
) -> ActivationResponse:
    """Route avec gestion automatique des dates d'activation."""

    for el in activation.activation_orders:
        if el.activation and (
            el.activation.request_soh_bib or el.activation.request_soh_oem
        ):
            start_date, end_date = await calculate_activation_dates(
                el.activation.request_soh_bib, el.activation.request_soh_oem
            )

            el.activation.start_date = start_date
            el.activation.end_date = end_date

    return await process_vehicle_activation(activation, user, db)


@router.post(
    "/activate",
    response_model=ActivationResponse,
    summary="Activate vehicles with manual date specification",
    description="Activate vehicles with manually specified start and end dates.",
    include_in_schema=False,
)
async def activate_vehicles(
    activation: ActivationRequest,
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
) -> ActivationResponse:
    response = await process_vehicle_activation(activation, user, db)

    return response


@router.delete(
    "/deactivate",
    response_model=DeactivationResponse,
    summary="Delete the requested activation of a vehicle",
    description="Delete the requested activation of a vehicle",
    include_in_schema=False,
)
async def delete_requested_activation(
    deactivation_request: DeactivationRequest,
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
) -> DeactivationResponse:
    # Verify user has access to the fleet
    has_access = await verify_user_fleet_access(
        db, user.id, uuid.UUID(deactivation_request.fleet_id)
    )
    if not has_access:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User does not have access to fleet {deactivation_request.fleet_id}",
        )

    # Get vehicles that belong to the user's fleet and match the provided VINs
    vehicles = await db.execute(
        select(Vehicle).where(
            Vehicle.vin.in_(deactivation_request.vins),
            Vehicle.fleet_id == uuid.UUID(deactivation_request.fleet_id),
        )
    )
    vehicles = vehicles.scalars().all()

    # Check if all requested VINs were found in the user's fleet
    found_vins = {vehicle.vin for vehicle in vehicles}
    requested_vins = set(deactivation_request.vins)
    missing_vins = requested_vins - found_vins

    if missing_vins:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vehicles with VINs {missing_vins} not found in fleet {deactivation_request.fleet_id}",
        )

    for vehicle in vehicles:
        if vehicle.activation_requested_status:
            vehicle.bib_report_requested_status = False
            vehicle.readout_report_requested_status = False
            vehicle.activation_requested_status = False

            vehicle.activation_status_message = "DEACTIVATION REQUESTED"
            # Choice to overwrite programmed deactivation date
            vehicle.activation_end_date = None
            vehicle.activation_start_date = None
            await db.commit()

            # Refresh vehicle to get updated state
            await db.refresh(vehicle)
            await _insert_vehicle_activation_history(db, vehicle)
        elif vehicle.activation_status:
            vehicle.bib_report_requested_status = False
            vehicle.readout_report_requested_status = False
            vehicle.activation_end_date = None
            vehicle.activation_start_date = None
            vehicle.activation_status_message = "DEACTIVATION REQUESTED"
            await db.commit()

            # Refresh vehicle to get updated state
            await db.refresh(vehicle)
            await _insert_vehicle_activation_history(db, vehicle)
        elif vehicle.readout_report_requested_status:
            vehicle.readout_report_requested_status = False
            vehicle.activation_status_message = "INSTANT READOUT NOT REQUESTED ANYMORE"
            await db.commit()

            # Refresh vehicle to get updated state
            await db.refresh(vehicle)
            await _insert_vehicle_activation_history(db, vehicle)
        else:
            vehicle.activation_status_message = "DEACTIVATED"
            await db.commit()
    return DeactivationResponse(
        vehicles=[
            VehicleStatus(
                vin=vehicle.vin or "",
                requested_soh_readout=False,
                requested_soh_bib=False,
                requested_activation=vehicle.activation_requested_status or False,
                status=vehicle.activation_status,
                message=vehicle.activation_status_message,
            )
            for vehicle in vehicles
        ]
    )
