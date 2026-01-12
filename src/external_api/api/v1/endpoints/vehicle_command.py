import logging
import uuid

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from activation.config.mappings import mapping_vehicle_type
from core.models.make import MakeEnum
from core.sql_utils import get_async_db
from db_models import Make, Oem
from db_models.fleet import Fleet, UserFleet
from db_models.vehicle import Battery, Vehicle, VehicleActivationHistory, VehicleModel
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.schemas.user import GetCurrentUser
from external_api.schemas.vehicle_command import (
    ActivationRequest,
    ActivationRequestVehicle,
    ActivationResponse,
    DeactivationRequest,
    DeactivationResponse,
    FleetInfo,
    FleetInfoResponse,
    MakeInfo,
    MakeInfoResponse,
    ModelInfo,
    ModelInfoResponse,
    VehicleStatus,
)

OEMS_W_READOUT_SOH = [
    MakeEnum.bmw.value,
    MakeEnum.tesla.value,
    MakeEnum.kia.value,
    MakeEnum.stellantis.value,
]
OEMS_W_BIB_SOH = [
    MakeEnum.mercedes_benz.value,
    MakeEnum.ford.value,
    MakeEnum.renault.value,
    MakeEnum.kia.value,
    MakeEnum.volvo_cars.value,
]
OEMS_W_SOH_W_MODEL_API = [
    MakeEnum.tesla.value,
    MakeEnum.bmw.value,
    MakeEnum.renault.value,
]
OEMS_W_SOH_WO_MODEL_API = [
    MakeEnum.mercedes_benz.value,
    MakeEnum.kia.value,
    MakeEnum.ford.value,
    MakeEnum.stellantis.value,
    MakeEnum.volvo_cars.value,
]

logger = logging.getLogger(__name__)
router = APIRouter()


async def verify_user_fleet_access(
    db: AsyncSession, user_id: uuid.UUID, fleet_id: uuid.UUID
) -> bool:
    """
    Verify that a user has access to a specific fleet.

    Args:
        db: Database session
        user_id: User ID to check
        fleet_id: Fleet ID to verify access to

    Returns:
        True if user has access to the fleet, False otherwise
    """
    result = await db.execute(
        select(UserFleet).where(
            UserFleet.user_id == user_id, UserFleet.fleet_id == fleet_id
        )
    )
    return result.scalar_one_or_none() is not None


async def _get_available_makes(db: AsyncSession) -> list[MakeInfo]:
    """
    Get the list of available makes from the database with their conditions.

    Returns:
        List of MakeInfo objects with make_id, make_name, and make_conditions
    """
    result = await db.execute(
        select(Make.id, Make.make_name, Oem.oem_name)
        .join(Oem, Make.oem_id == Oem.id)
        .where(Oem.oem_name.in_(OEMS_W_SOH_WO_MODEL_API + OEMS_W_SOH_W_MODEL_API))
        .order_by(Make.make_name)
    )

    rows = result.all()

    make_infos = []
    for row in rows:
        make_id, make_name, oem_name = row

        soh_readout = False
        soh_bib = False

        # Determine conditions based on OEM
        if oem_name in OEMS_W_READOUT_SOH:
            soh_readout = True
        if oem_name in OEMS_W_BIB_SOH:
            soh_bib = True

        if oem_name in OEMS_W_SOH_WO_MODEL_API:
            make_conditions = "Model & type required"
        else:
            make_conditions = None

        make_infos.append(
            MakeInfo(
                make_id=str(make_id),
                make_name=make_name,
                make_conditions=make_conditions,
                soh_readout=soh_readout,
                soh_bib=soh_bib,
            )
        )

    return make_infos


async def _insert_vehicle_activation_history(
    db: AsyncSession, vehicle: Vehicle, oem_detail: str | None = None
):
    vehicle_activation_history = VehicleActivationHistory(
        vehicle_id=vehicle.id,
        activation_requested_status=vehicle.activation_requested_status,
        activation_status=vehicle.activation_status,
        activation_status_message=vehicle.activation_status_message,
        oem_detail=oem_detail,
    )
    db.add(vehicle_activation_history)
    await db.commit()


async def get_vehicle_model_id_from_schema(
    db: AsyncSession, vehicle_schema: ActivationRequestVehicle
) -> uuid.UUID | None:
    """
    Get vehicle model ID from a Pydantic schema.

    Args:
        db: Database session
        vehicle_schema: Pydantic Vehicle schema from request

    Returns:
        Vehicle model ID if found, None otherwise
    """
    if vehicle_schema.make and vehicle_schema.model and vehicle_schema.type:
        # Query all vehicle models with joins for make and battery info
        query = (
            select(
                VehicleModel.model_name,
                VehicleModel.id,
                VehicleModel.type,
                VehicleModel.commissioning_date,
                VehicleModel.end_of_life_date,
                Make.make_name,
                Battery.capacity,
            )
            .join(Make, VehicleModel.make_id == Make.id, isouter=True)
            .join(Battery, VehicleModel.battery_id == Battery.id, isouter=True)
        )

        result = await db.execute(query)
        rows = result.fetchall()

        # Convert to DataFrame for mapping_vehicle_type function
        model_existing = pd.DataFrame(
            [
                {
                    "model_name": row.model_name,
                    "id": str(row.id),
                    "type": row.type,
                    "commissioning_date": row.commissioning_date,
                    "end_of_life_date": row.end_of_life_date,
                    "make_name": row.make_name,
                    "capacity": float(row.capacity) if row.capacity else None,
                }
                for row in rows
            ]
        )

        vehicle_model_id = mapping_vehicle_type(
            vehicle_schema.type,
            vehicle_schema.make.lower(),
            vehicle_schema.model.lower(),
            model_existing,
        )
    else:
        vehicle_model_id = None
    return vehicle_model_id


@router.get(
    "/makes",
    response_model=MakeInfoResponse,
    summary="Get all available vehicle makes",
    description="Get the list of all vehicle makes available in the database. Use these makes when activating vehicles.",
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


@router.post(
    "/activate",
    response_model=ActivationResponse,
    summary="Activate one or more vehicles",
    description="Activate one or more vehicles by providing their VINs, make, model, and type. Optionally, include comments for tracking, scheduled activation dates, and deactivation dates.",
)
async def activate_vehicles(
    activation: ActivationRequest,
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
) -> ActivationResponse:
    # Verify user has access to the fleet
    has_access = await verify_user_fleet_access(
        db, user.id, uuid.UUID(activation.fleet_id)
    )

    if not has_access:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User does not have access to fleet {activation.fleet_id}",
        )

    available_makes = await _get_available_makes(db)
    available_make_names = [make.make_name for make in available_makes]

    response = ActivationResponse(
        vehicles=[],
    )

    for el in activation.activation_orders:
        # Validate that the make exists in available makes
        if el.vehicle.make not in available_make_names:
            response.vehicles.append(
                VehicleStatus(
                    vin=el.vehicle.vin,
                    requested_status=False,
                    status=False,
                    message=f"Make '{el.vehicle.make}' not found in database. Please use the /makes endpoint to get the list of available makes.",
                )
            )
            continue

        # Check if model and type are required for this make
        make_info = next(
            (m for m in available_makes if m.make_name == el.vehicle.make), None
        )

        if (
            make_info
            and make_info.make_conditions
            and (not el.vehicle.model or not el.vehicle.type)
        ):
            response.vehicles.append(
                VehicleStatus(
                    vin=el.vehicle.vin,
                    requested_status=False,
                    status=False,
                    message=f"Model and type are required for {el.vehicle.make} vehicles in order to get the battery report.",
                )
            )
            continue

        # Check if the VIN is already in database
        vehicle = await db.execute(select(Vehicle).where(Vehicle.vin == el.vehicle.vin))
        vehicle = vehicle.scalar_one_or_none()

        # Retrieve status of the vehicle if exists else insert
        if vehicle:
            if vehicle.activation_status:
                # In case user added new model to the vehicle
                vehicle_model_id = await get_vehicle_model_id_from_schema(
                    db, el.vehicle
                )
                vehicle.activation_start_date = (
                    el.activation.start_date if el.activation else None
                )
                vehicle.activation_end_date = (
                    el.activation.end_date if el.activation else None
                )
                vehicle.vehicle_model_id = vehicle_model_id
                vehicle.is_processed = False  # reset is_processed
                await db.commit()
                response.vehicles.append(
                    VehicleStatus(
                        vin=el.vehicle.vin,
                        requested_status=True,
                        status=vehicle.activation_status
                        if vehicle.activation_status
                        else False,
                        message="ALREADY ACTIVATED",
                    )
                )
                continue
            else:
                vehicle_model_id = await get_vehicle_model_id_from_schema(
                    db, el.vehicle
                )
                vehicle.vehicle_model_id = vehicle_model_id
                vehicle.is_processed = False  # reset is_processed
                vehicle.activation_start_date = (
                    el.activation.start_date if el.activation else None
                )
                vehicle.activation_end_date = (
                    el.activation.end_date if el.activation else None
                )
                vehicle.activation_requested_status = True
                vehicle.activation_status_message = "ACTIVATION REQUESTED"
                vehicle.is_processed = False  # reset is_processed
                await _insert_vehicle_activation_history(db, vehicle, None)
                await db.commit()

                response.vehicles.append(
                    VehicleStatus(
                        vin=el.vehicle.vin,
                        requested_status=vehicle.activation_requested_status or False,
                        status=vehicle.activation_status,
                        message=vehicle.activation_status_message,
                        comment=vehicle.activation_comment,
                    )
                )
        else:
            vehicle_model_id = await get_vehicle_model_id_from_schema(db, el.vehicle)

            await db.execute(
                insert(Vehicle).values(
                    vin=el.vehicle.vin,
                    fleet_id=activation.fleet_id,
                    region_id="4032de92-ae96-4061-980e-2d633f7228a8",  # A modifier avec l'ID neutre de la  base de prod
                    vehicle_model_id=vehicle_model_id,
                    activation_start_date=el.activation.start_date
                    if el.activation
                    else None,
                    activation_end_date=el.activation.end_date
                    if el.activation
                    else None,
                    activation_requested_status=True,
                    activation_status=False,
                    activation_comment=el.comment if el.comment else None,
                    activation_status_message="ACTIVATION REQUESTED",
                    is_processed=False,
                )
            )
            await db.commit()

            # Get the newly inserted vehicle
            result = await db.execute(
                select(Vehicle).where(Vehicle.vin == el.vehicle.vin)
            )
            vehicle = result.scalar_one_or_none()

            if vehicle:
                await _insert_vehicle_activation_history(db, vehicle, None)

            response.vehicles.append(
                VehicleStatus(
                    vin=el.vehicle.vin,
                    requested_status=True,
                    status=False,
                    message="ACTIVATION REQUESTED",
                )
            )
            continue

    return response


@router.get(
    "/fleets",
    response_model=FleetInfoResponse,
    summary="Get the fleets of the account",
    description="Get the fleets of the account",
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
    "/status",
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
            requested_status=vehicle.activation_requested_status or False,
            status=vehicle.activation_status,
            message=vehicle.activation_status_message,
            comment=vehicle.activation_comment,
            make=make.make_name if make else None,
            model_name=model.model_name if model else None,
            type=model.type if model else None,
            version=model.version if model else None,
        )
        for vehicle, model, make in vehicles_data
    ]


@router.delete(
    "/deactivate",
    response_model=DeactivationResponse,
    summary="Delete the requested activation of a vehicle",
    description="Delete the requested activation of a vehicle",
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
            vehicle.activation_end_date = None
            vehicle.activation_start_date = None
            vehicle.activation_status_message = "DEACTIVATION REQUESTED"
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
                requested_status=False,
                status=vehicle.activation_status,
                message=vehicle.activation_status_message,
            )
            for vehicle in vehicles
        ]
    )
