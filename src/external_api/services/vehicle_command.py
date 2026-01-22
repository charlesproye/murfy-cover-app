import uuid
from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from activation.config.mappings import mapping_vehicle_type
from core.models.make import MakeEnum
from db_models import Make, Oem
from db_models.vehicle import Battery, Vehicle, VehicleActivationHistory, VehicleModel
from external_api.core.utils import verify_user_fleet_access
from external_api.schemas.user import GetCurrentUser
from external_api.schemas.vehicle_command import (
    ActivationOrder,
    ActivationRequest,
    ActivationRequestVehicle,
    ActivationResponse,
    MakeInfo,
    VehicleStatus,
)

# OEMs SoH
OEMS_W_INSTANT_READOUT_SOH = [
    MakeEnum.tesla.value,
]

OEMS_W_ACTIVATED_READOUT_SOH = [
    MakeEnum.kia.value,
    MakeEnum.stellantis.value,
    MakeEnum.bmw.value,
]

OEMS_W_READOUT_SOH = OEMS_W_INSTANT_READOUT_SOH + OEMS_W_ACTIVATED_READOUT_SOH

OEMS_W_BIB_SOH = [
    MakeEnum.mercedes_benz.value,
    MakeEnum.tesla.value,
    MakeEnum.ford.value,
    MakeEnum.renault.value,
    MakeEnum.kia.value,
]

# Static API
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
]

# Consent information
OEMS_W_CONSENT = {
    MakeEnum.tesla.value: "Business Account : Please send the email of the owner of the vehicle to martin@bib-batteries.fr to get the consent. Personal Account : Follow this link https://get-evalue.com/tesla-activation to get the consent.",
    MakeEnum.ford.value: "Please send the email of the owner of the vehicle to the OEM to get the consent of Ford vehicles.",
}


async def _is_activation_requested(el: ActivationOrder) -> bool:
    return not (
        el.vehicle.make in OEMS_W_INSTANT_READOUT_SOH
        and not el.activation.request_soh_bib
    )


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


async def process_vehicle_activation(
    activation: ActivationRequest,
    user: GetCurrentUser,
    db: AsyncSession,
) -> ActivationResponse:
    """
    Logique commune de traitement des activations.
    Cette fonction contient tout le code original de activate_vehicles.
    """
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
                    requested_soh_readout=False,
                    requested_soh_bib=False,
                    requested_activation=False,
                    status=False,
                    message=f"Make '{el.vehicle.make}' not found in database. Please use the /makes endpoint to get the list of available makes.",
                )
            )
            continue

        if el.vehicle.make == "stellantis" and el.vehicle.vin[7] == "X":
            response.vehicles.append(
                VehicleStatus(
                    vin=el.vehicle.vin,
                    requested_soh_readout=False,
                    requested_soh_bib=False,
                    requested_activation=False,
                    status=False,
                    message=f"{el.vehicle.make.capitalize()} with X as 8th VIN character does not provide SoH (old telemetry hardware).",
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
                    requested_soh_readout=False,
                    requested_soh_bib=False,
                    requested_activation=False,
                    status=False,
                    message=f"Model and type are required for {el.vehicle.make} vehicles in order to get the battery report.",
                )
            )
            continue

        # Check if the VIN is already in database
        vehicle = await db.execute(select(Vehicle).where(Vehicle.vin == el.vehicle.vin))
        vehicle = vehicle.scalar_one_or_none()

        # Retrieve status of the vehicle if exists else insert
        if el.activation and (
            el.activation.request_soh_bib or el.activation.request_soh_oem
        ):
            consent_information = OEMS_W_CONSENT.get(el.vehicle.make, "")
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

                    vehicle.bib_report_requested_status = (
                        el.activation.request_soh_bib or False
                    )
                    vehicle.readout_report_requested_status = (
                        el.activation.request_soh_oem or False
                    )

                    vehicle.vehicle_model_id = vehicle_model_id
                    vehicle.is_processed = False  # reset is_processed
                    await db.commit()
                    response.vehicles.append(
                        VehicleStatus(
                            vin=el.vehicle.vin,
                            requested_soh_readout=el.activation.request_soh_oem
                            or False,
                            requested_soh_bib=el.activation.request_soh_bib or False,
                            requested_activation=True,
                            status=vehicle.activation_status
                            if vehicle.activation_status
                            else False,
                            message="ALREADY ACTIVATED",
                            comment=el.comment if el.comment else None,
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
                    vehicle.bib_report_requested_status = (
                        el.activation.request_soh_bib or False
                    )
                    vehicle.readout_report_requested_status = (
                        el.activation.request_soh_oem or False
                    )
                    vehicle.activation_requested_status = True
                    vehicle.activation_status_message = "ACTIVATION REQUESTED"

                    if not await _is_activation_requested(el):
                        vehicle.activation_status_message = "INSTANT READOUT REQUESTED"
                        vehicle.activation_requested_status = False

                    vehicle.is_processed = False  # reset is_processed
                    await _insert_vehicle_activation_history(db, vehicle, None)
                    await db.commit()

                    response.vehicles.append(
                        VehicleStatus(
                            vin=el.vehicle.vin,
                            requested_soh_readout=vehicle.readout_report_requested_status
                            or False,
                            requested_soh_bib=vehicle.bib_report_requested_status
                            or False,
                            requested_activation=vehicle.activation_requested_status
                            or False,
                            status=vehicle.activation_status,
                            message=vehicle.activation_status_message,
                            comment=vehicle.activation_comment,
                            consent_information=consent_information,
                        )
                    )
            else:
                vehicle_model_id = await get_vehicle_model_id_from_schema(
                    db, el.vehicle
                )

                if not await _is_activation_requested(el):
                    activation_status_message = "INSTANT READOUT REQUESTED"
                    activation_requested_status = False
                else:
                    activation_status_message = "ACTIVATION REQUESTED"
                    activation_requested_status = True

                new_vehicle = Vehicle(
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
                    bib_report_requested_status=el.activation.request_soh_bib,
                    readout_report_requested_status=el.activation.request_soh_oem,
                    activation_requested_status=activation_requested_status,
                    activation_status=False,
                    activation_comment=el.comment if el.comment else None,
                    activation_status_message=activation_status_message,
                    is_processed=False,
                )
                db.add(new_vehicle)

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
                        requested_soh_readout=el.activation.request_soh_oem or False,
                        requested_soh_bib=el.activation.request_soh_bib or False,
                        requested_activation=activation_requested_status or False,
                        status=False,
                        message=activation_status_message,
                        comment=el.comment if el.comment else None,
                        consent_information=consent_information,
                    )
                )
                continue
        else:
            response.vehicles.append(
                VehicleStatus(
                    vin=el.vehicle.vin,
                    requested_soh_readout=False,
                    requested_soh_bib=False,
                    requested_activation=False,
                    status=False,
                    message="No SoH requested in the request",
                    comment=el.comment if el.comment else None,
                )
            )
            continue

    return response


async def calculate_activation_dates(
    request_soh_bib: bool, request_soh_oem: bool
) -> tuple[datetime, datetime]:
    now = datetime.now()
    start_date = now

    # Case readout only
    if request_soh_oem and not request_soh_bib:
        # Fin du mois en cours - 1 jour
        next_month = now.replace(day=28) + timedelta(days=4)
        end_of_month = next_month - timedelta(days=next_month.day)
        end_date = end_of_month - timedelta(days=1)

    # Case BIB only or both (BIB + OEM)
    else:
        if now.day > 10:
            # Après le 10: fin du mois suivant - 1 jour
            next_month = now + relativedelta(months=1)
            end_of_next_month = next_month.replace(day=28) + timedelta(days=4)
            end_of_next_month = end_of_next_month - timedelta(
                days=end_of_next_month.day
            )
            end_date = end_of_next_month - timedelta(days=1)
        else:
            # Après le 10: fin du mois en cours - 1 jour
            next_month = now.replace(day=28) + timedelta(days=4)
            end_of_month = next_month - timedelta(days=next_month.day)
            end_date = end_of_month - timedelta(days=1)

    return start_date, end_date
