"""Endpoints for vehicle data access"""

import logging
import time
import uuid
from typing import Any

from external_api.core.cookie_auth import get_current_user_from_cookie
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.utils import add_vehicles_to_google_sheet
from external_api.db.session import get_db
from external_api.schemas.user import User
from external_api.schemas.vehicle import (
    DynamicVehicleData,
    VehicleActivationRequest,
    VehicleActivationResponse,
    VehicleEligibilityResponse,
)
from external_api.services.api_pricing import get_api_user_pricing, log_api_call
from external_api.services.redis import (
    add_distinct_vin_and_check_limit,
    increment_and_check_rate_limit,
)
from external_api.services.vehicle import (
    check_vehicle_eligibility,
    get_dynamic_vehicle_data,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def is_tesla_vin(vin: str) -> bool:
    """
    Detects if a VIN belongs to a Tesla vehicle.

    Tesla VINs typically start with:
    - 5YJ for vehicles manufactured in the United States (Model S, 3, X, Y)
    - 7SA for vehicles manufactured in China
    - LRW for some Chinese vehicles
    - SFZ for some European vehicles (Berlin)

    Args:
        vin: Vehicle Identification Number

    Returns:
        bool: True if the VIN corresponds to a Tesla vehicle, False otherwise
    """
    if len(vin) < 3:
        return False

    tesla_prefixes = ["5YJ", "7SA", "LRW", "SFZ", "XP7"]
    return vin[:3].upper() in tesla_prefixes


async def check_rate_limit(
    vin: str,
    endpoint: str,
    user: User = Depends(get_current_user_from_cookie()),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Checks rate limits and logs API calls.
    Checks two limits:
    1. Total number of requests per day
    2. Number of distinct VINs per day
    """
    start_time = time.time()

    try:
        # Get pricing plan
        pricing_info = await get_api_user_pricing(db, user.id)

        # 1. Check total requests limit
        current_count, limit_exceeded = await increment_and_check_rate_limit(
            user.id, pricing_info["requests_limit"]
        )

        if limit_exceeded:
            logger.warning(
                f"Request limit exceeded for API user {user.id}, endpoint {endpoint}"
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Daily request limit exceeded. Please upgrade your plan or try again tomorrow.",
            )

        # 2. Check distinct VINs limit
        distinct_count, vin_limit_exceeded = await add_distinct_vin_and_check_limit(
            user.id, vin, pricing_info["max_distinct_vins"]
        )

        if vin_limit_exceeded:
            logger.warning(f"Distinct VINs limit exceeded for API user {user.id}")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Distinct VINs limit exceeded ({pricing_info['max_distinct_vins']}). Please upgrade your plan or try again tomorrow.",
            )

        # Calculate response time
        response_time = (time.time() - start_time) * 1000  # in milliseconds

        # Log this API call
        await log_api_call(db, user.id, endpoint, vin, response_time=response_time)

        logger.info(
            f"API Call logged: API user {user.id}, endpoint {endpoint}, "
            f"VIN {vin}, requests {current_count}/{pricing_info['requests_limit']}, "
            f"distinct VINs {distinct_count}/{pricing_info['max_distinct_vins']}"
        )
    except Exception as e:
        # En cas d'erreur (notamment si l'utilisateur n'existe pas), on continue sans les limites
        logger.warning(
            f"Error during rate limit check: {e!s}. Continuing without limits."
        )
        response_time = (time.time() - start_time) * 1000
        logger.info(
            f"API Call logged (without limits): API user {user.id}, endpoint {endpoint}, "
            f"VIN {vin}"
        )


@router.get("/check-eligibility/{vin}", response_model=VehicleEligibilityResponse)
async def check_vehicle_eligibility_endpoint(
    vin: str = Path(
        ..., description="VIN of the vehicle to check", min_length=10, max_length=25
    ),
    user: User = Depends(get_current_user_from_cookie()),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Check the eligibility of a vehicle for activation.

    - **vin**: Vehicle Identification Number (VIN)

    Returns the eligibility status of the vehicle and information about its status.

    If the vehicle does not exist in the database, it is recommended to proceed with its activation.
    """
    logger.info(f"Checking vehicle {vin} eligibility for user {user.id}")

    try:
        # Check VIN format
        if len(vin) < 10 or len(vin) > 25:
            return VehicleEligibilityResponse(
                vin=vin,
                exists=False,
                is_eligible=False,
                is_activated=False,
                message="Invalid VIN format. The VIN must contain between 10 and 25 characters.",
            )

        # Use existing function to check eligibility
        eligibility = await check_vehicle_eligibility(db, vin)

        # Build response based on result
        if eligibility["exists"]:
            if eligibility["is_eligible"]:
                if eligibility["is_activated"]:
                    message = "This vehicle is eligible and activated."
                else:
                    message = "This vehicle is eligible for activation and is not yet activated."
            else:
                message = "This vehicle is not eligible for activation."
        else:
            message = "This vehicle does not exist in our database. You can proceed with its activation to verify its eligibility."

        return VehicleEligibilityResponse(
            vin=vin,
            exists=eligibility["exists"],
            is_eligible=eligibility["is_eligible"],
            is_activated=eligibility["is_activated"],
            message=message,
        )

    except Exception as e:
        # Capture and log complete error
        error_detail = str(e)
        logger.error(
            f"Error during vehicle eligibility check for {vin}: {error_detail}"
        )

        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during vehicle eligibility check: {error_detail}",
        )


@router.post("/activate", response_model=VehicleActivationResponse)
async def activate_vehicles(
    activation: VehicleActivationRequest,
    user: User = Depends(get_current_user_from_cookie()),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Initiates the vehicle activation process for the specified vehicles.

    - **vins** : List of VINs to activate

    Returns the list of VINs processed, the status of the operation, and the error details by VIN.

    Notes:
    - The activation process can take up to 24h to complete.
    - Vehicles must be eligible to be activated.
    - Submitted VINs must all belong to the same brand.
    - For Tesla vehicles, only one VIN must be submitted at a time.
    """
    logger.info(
        f"Activation request for {len(activation.vins)} vehicles by user {user.id}"
    )
    logger.info(
        f"VINs to activate: {', '.join(activation.vins[:5])}{'...' if len(activation.vins) > 5 else ''}"
    )

    try:
        # Check validity of VINs
        invalid_vins = []
        already_activated_vins = []
        not_eligible_vins = []
        pending_activation_vins = []
        new_vins = []
        tesla_vins = []

        # Dictionary to store error details by VIN
        errors_by_vin = {}
        # Check if Tesla VINs are present
        for vin in activation.vins:
            if is_tesla_vin(vin):
                tesla_vins.append(vin)

        # If Tesla vehicles are present, check that there is only one VIN at the total
        if tesla_vins and len(activation.vins) > 1:
            logger.warning(
                f"Attempt to activate Tesla with other vehicles: {len(activation.vins)} vehicles including {len(tesla_vins)} Tesla"
            )
            return VehicleActivationResponse(
                vins=activation.vins,
                success=False,
                message="Tesla vehicles must be activated individually, one VIN at a time. Please submit only the Tesla VIN without other vehicles.",
                errors=dict.fromkeys(
                    activation.vins, "Tesla vehicles require individual activation"
                ),
            )

        # Normal VIN processing
        for vin in activation.vins:
            # Check VIN format
            if len(vin) < 10 or len(vin) > 25:
                invalid_vins.append(vin)
                errors_by_vin[vin] = "Invalid VIN format"
                continue

            # Check eligibility and activation status
            eligibility = await check_vehicle_eligibility(db, vin)
            if eligibility["exists"]:
                if eligibility["is_eligible"]:
                    if eligibility["is_activated"]:
                        # Vehicle exists, is eligible, but already activated
                        already_activated_vins.append(vin)
                        errors_by_vin[vin] = "Vehicle already activated"
                    else:
                        # Vehicle exists, is eligible, but not yet activated
                        pending_activation_vins.append(vin)
                        # No error for vehicles pending activation
                else:
                    # Vehicle exists but is not eligible
                    not_eligible_vins.append(vin)
                    errors_by_vin[vin] = "Vehicle not eligible for activation"
            else:
                # Vehicle doesn't exist in DB, we'll try to activate it
                new_vins.append(vin)
                # No error for new vehicles

        # Variable to store Tesla authentication URL if necessary
        tesla_auth_url = None

        # Special processing for a Tesla
        if len(tesla_vins) == 1:
            tesla_vin = tesla_vins[0]
            logger.info(f"Activation of a Tesla vehicle with VIN {tesla_vin}")

            try:
                # Generate a UUID for the Tesla user
                tesla_user_id = str(uuid.uuid4())

                # Get user information from public schema
                result = await db.execute(
                    text("""
                        SELECT first_name, last_name, email
                        FROM public.user
                        WHERE id = :user_id
                    """),
                    {"user_id": user.id},
                )
                user_info = result.fetchone()
                if not user_info:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="User information not found",
                    )

                full_name = f"{user_info[0]} {user_info[1]}".strip()
                user_email = user_info[2]

                # Create Tesla authentication URL with the generated ID as state parameter
                tesla_auth_url = f"https://auth.tesla.com/oauth2/v3/authorize?client_id=8832277ae4cc-4461-8396-127310129dc6&response_type=code&redirect_uri=https://bib-data.fr/v1/allowed_redirect&scope=openid%20offline_access%20user_data%20vehicle_device_data%20energy_device_data&state={tesla_user_id}"

                # First, check if the VIN already exists
                result = await db.execute(
                    text("SELECT id FROM tesla.user WHERE vin = :vin"),
                    {"vin": tesla_vin},
                )
                existing_record = result.fetchone()

                if existing_record:
                    # Update existing record
                    await db.execute(
                        text("""
                            UPDATE tesla.user
                            SET full_name = :full_name, email = :email
                            WHERE vin = :vin
                        """),
                        {"vin": tesla_vin, "full_name": full_name, "email": user_email},
                    )
                else:
                    # Insert new record
                    await db.execute(
                        text("""
                            INSERT INTO tesla.user (id, vin, full_name, email)
                            VALUES (:id, :vin, :full_name, :email)
                        """),
                        {
                            "id": tesla_user_id,
                            "vin": tesla_vin,
                            "full_name": full_name,
                            "email": user_email,
                        },
                    )

                await db.commit()
                logger.info(
                    f"Tesla vehicle with VIN {tesla_vin} recorded in tesla.user"
                )

            except Exception as e:
                await db.rollback()
                logger.error(f"Error during insertion in tesla.user: {e!s}")
                # Continue despite error, in case the table already exists

        # Vehicles to activate = new vehicles + those pending activation
        valid_vins = new_vins + pending_activation_vins

        # Build response message based on cases
        message_parts = []

        if invalid_vins:
            logger.warning(f"Invalid VINs detected: {', '.join(invalid_vins)}")
            message_parts.append(f"{len(invalid_vins)} invalid VIN(s) detected")

        if not_eligible_vins:
            logger.warning(
                f"Non-eligible VINs detected: {', '.join(not_eligible_vins)}"
            )
            message_parts.append(
                f"{len(not_eligible_vins)} vehicle(s) not eligible for activation"
            )

        if already_activated_vins:
            logger.info(f"Already activated VINs: {', '.join(already_activated_vins)}")
            message_parts.append(
                f"{len(already_activated_vins)} vehicle(s) already activated"
            )

        if pending_activation_vins:
            logger.info(
                f"VINs pending activation: {', '.join(pending_activation_vins)}"
            )
            message_parts.append(
                f"{len(pending_activation_vins)} eligible vehicle(s) pending activation"
            )

        if new_vins:
            logger.info(f"New VINs to activate: {', '.join(new_vins)}")
            message_parts.append(
                f"{len(new_vins)} new vehicle(s) being verified for activation"
            )

        if not valid_vins:
            # No valid VINs to activate
            return VehicleActivationResponse(
                vins=activation.vins,
                success=False,
                message="No vehicles could be activated. " + " | ".join(message_parts),
                errors=errors_by_vin,
            )

        # Add valid vehicles to Google Sheet for activation
        logger.info(
            f"Attempting activation for {len(valid_vins)} vehicles for user {user.id}"
        )
        success = await add_vehicles_to_google_sheet(valid_vins, str(user.id), db)

        # Success log
        logger.info(
            f"Activation request successfully submitted for {len(valid_vins)} vehicles"
        )

        # Final message with number of vehicles being activated
        if message_parts:
            message = (
                f"Activation of {len(valid_vins)} vehicle(s) has been initiated and will be completed within 24h. "
                + " | ".join(message_parts)
            )
        else:
            message = f"Activation of {len(valid_vins)} vehicle(s) has been initiated and will be completed within 24h."

        # Add Tesla authentication information if necessary
        if tesla_auth_url:
            # For Tesla, completely replace standard message with specific instructions
            # Format adapted to be readable even if \n is not interpreted correctly
            message = (
                "To activate your Tesla vehicle, follow these steps: "
                + "(1) Click on the following link to authorize access to your Tesla account: "
                + tesla_auth_url
                + " "
                + "(2) Once consent is granted, visit https://www.tesla.com/_ak/bib-data.fr "
                + "(3) Add the virtual key to your Tesla mobile application."
            )

        return VehicleActivationResponse(
            vins=activation.vins, success=success, message=message, errors=errors_by_vin
        )

    except HTTPException as e:
        # Relaunch HTTPException as they are, to preserve status and error message
        logger.error(
            f"HTTP error during vehicle activation: {e.detail}, status_code={e.status_code}"
        )
        raise
    except Exception as e:
        # Capture and log complete error
        error_detail = str(e)
        logger.error(f"Unexpected error during vehicle activation: {error_detail}")

        # Optionally, you can log complete trace for better debugging
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during vehicle activation: {error_detail}",
        )


# @router.get("/static/{vin}", response_model=StaticVehicleData)
# async def get_static_vehicle(
#     vin: str = Path(..., description="VIN of the vehicle", min_length=10, max_length=25),
#     db: AsyncSession = Depends(get_db),
#     user: User = Depends(get_current_user),
# ) -> Any:
#     """
#     Get static data for a vehicle by its VIN.

#     - **vin**: Vehicle Identification Number (VIN)

#     Returns the model, version, capacity, warranty, trendline, and other data of the vehicle.

#     """
#     logger.info(f"Request for static data for VIN {vin} by API user {user.id}")

#     # Check usage limits
#     await check_rate_limit(vin, "/static", user, db)

#     # First, check existence and eligibility of the vehicle
#     eligibility = await check_vehicle_eligibility(db, vin)

#     if not eligibility["exists"]:
#         logger.warning(f"Vehicle with VIN {vin} not found in database")
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="Vehicle not found. Use the /activate endpoint to activate this vehicle."
#         )

#     if not eligibility["is_eligible"]:
#         logger.warning(f"Vehicle with VIN {vin} not eligible for data access")
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="This vehicle is not eligible for data access."
#         )

#     if not eligibility["is_activated"]:
#         logger.warning(f"Vehicle with VIN {vin} not activated")
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="This vehicle is not yet activated. Use the /activate endpoint to activate it."
#         )

#     # Check if user has access to this vehicle (same fleet_id)
#     try:
#         # Get vehicle fleet_id
#         result = await db.execute(
#             text("SELECT fleet_id FROM vehicle WHERE vin = :vin"),
#             {"vin": vin}
#         )
#         vehicle_data = result.fetchone()

#         if not vehicle_data:
#             logger.warning(f"Vehicle with VIN {vin} not found in vehicle table")
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="Vehicle not found in database."
#             )

#         vehicle_fleet_id = str(vehicle_data[0])
#         logger.debug(f"Fleet ID of vehicle {vin}: {vehicle_fleet_id}")

#         # Check if user has access to this fleet via user_fleet table
#         # Use user_id instead of id for access rights check
#         user_id_str = str(user.id)
#         logger.debug(f"Access rights check for user {user_id_str} on fleet {vehicle_fleet_id}")

#         query = """
#             SELECT 1 FROM user_fleet
#             WHERE user_id::text = :user_id
#             AND fleet_id::text = :fleet_id
#         """

#         result = await db.execute(
#             text(query),
#             {"user_id": user_id_str, "fleet_id": vehicle_fleet_id}
#         )
#         access_granted = result.fetchone()

#         if not access_granted:
#             # Log values for debugging
#             logger.warning(f"Access denied: user {user_id_str} attempting to access vehicle {vin} (fleet {vehicle_fleet_id})")

#             # Check if user exists in user_fleet
#             check_query = "SELECT fleet_id FROM user_fleet WHERE user_id::text = :user_id"
#             result = await db.execute(text(check_query), {"user_id": user_id_str})
#             user_fleets = [str(row[0]) for row in result.fetchall()]

#             if user_fleets:
#                 logger.debug(f"User {user_id_str} has access to fleets: {', '.join(user_fleets)}")
#             else:
#                 logger.debug(f"User {user_id_str} has no access to any fleet in user_fleet table")

#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail=f"You do not have access rights to this vehicle. It does not belong to any of your authorized fleets."
#             )

#         logger.debug(f"Access granted for user {user_id_str} to vehicle {vin}")

#     except HTTPException:
#         # Relaunch HTTPException as they are
#         raise
#     except Exception as e:
#         logger.error(f"Error during access rights check: {str(e)}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Error during access rights check."
#         )

#     # Get vehicle data
#     try:
#         vehicle_data = await get_static_vehicle_data(db, vin)
#         return vehicle_data
#     except HTTPException as e:
#         if e.status_code == status.HTTP_404_NOT_FOUND:
#             # If get_static_vehicle_data returns 404 while the vehicle exists, it's because there are no data
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="Static data not available for this vehicle."
#             )
#         raise


@router.get("/{vin}", response_model=DynamicVehicleData)
async def get_dynamic_vehicle(
    vin: str = Path(
        ..., description="VIN of the vehicle", min_length=10, max_length=25
    ),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user_from_cookie()),
) -> Any:
    """
    Get dynamic data for a vehicle by its VIN.

    - **vin**: Vehicle Identification Number (VIN)

    Returns the vehicle's health status, mileage, and additional data.

    Note: The vehicle must be activated, eligible, and belong to the same fleet as the user to access its data.
    If the vehicle is not found, you can activate it via the /activate endpoint.
    """
    logger.info(f"Request for dynamic data for VIN {vin} by API user {user.id}")

    # Check usage limits
    await check_rate_limit(vin, "/dynamic", user, db)

    # First check existence and eligibility of the vehicle
    eligibility = await check_vehicle_eligibility(db, vin)

    if not eligibility["exists"]:
        logger.warning(f"Vehicle with VIN {vin} not found in database")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found. Use the /activate endpoint to activate this vehicle.",
        )

    if not eligibility["is_eligible"]:
        logger.warning(f"Vehicle with VIN {vin} not eligible for data access")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This vehicle is not eligible for data access.",
        )

    if not eligibility["is_activated"]:
        logger.warning(f"Vehicle with VIN {vin} not activated")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This vehicle is not yet activated. Use the /activate endpoint to activate it.",
        )

    # Check if user has access to this vehicle (same fleet_id)
    try:
        # Get vehicle fleet_id
        result = await db.execute(
            text("SELECT fleet_id FROM vehicle WHERE vin = :vin"), {"vin": vin}
        )
        vehicle_data = result.fetchone()

        if not vehicle_data:
            logger.warning(f"Vehicle with VIN {vin} not found in vehicle table")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Vehicle not found in database.",
            )

        vehicle_fleet_id = str(vehicle_data[0])
        logger.debug(f"Fleet ID of vehicle {vin}: {vehicle_fleet_id}")

        # Check if user has access to this fleet via user_fleet table
        # Use user_id instead of id for access rights check
        user_id_str = str(user.id)
        logger.debug(
            f"Access rights check for user {user_id_str} on fleet {vehicle_fleet_id}"
        )

        query = """
            SELECT 1 FROM user_fleet
            WHERE user_id::text = :user_id
            AND fleet_id::text = :fleet_id
        """

        result = await db.execute(
            text(query), {"user_id": user_id_str, "fleet_id": vehicle_fleet_id}
        )
        access_granted = result.fetchone()

        if not access_granted:
            # Log values for debugging
            logger.warning(
                f"Access denied: user {user_id_str} attempting to access vehicle {vin} (fleet {vehicle_fleet_id})"
            )

            # Check if user exists in user_fleet
            check_query = (
                "SELECT fleet_id FROM user_fleet WHERE user_id::text = :user_id"
            )
            result = await db.execute(text(check_query), {"user_id": user_id_str})
            user_fleets = [str(row[0]) for row in result.fetchall()]

            if user_fleets:
                logger.debug(
                    f"User {user_id_str} has access to fleets: {', '.join(user_fleets)}"
                )
            else:
                logger.debug(
                    f"User {user_id_str} has no access to any fleet in user_fleet table"
                )

            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have access rights to this vehicle. It does not belong to any of your authorized fleets.",
            )

        logger.debug(f"Access granted for user {user_id_str} to vehicle {vin}")

    except HTTPException:
        # Re-raise HTTPException as is
        raise
    except Exception as e:
        logger.error(f"Error during access rights check: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during access rights check.",
        )

    # Get vehicle data
    try:
        vehicle_data = await get_dynamic_vehicle_data(db, vin)
        return vehicle_data
    except HTTPException as e:
        if e.status_code == status.HTTP_404_NOT_FOUND:
            # If get_dynamic_vehicle_data returns 404 while the vehicle exists, it means there is no data
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dynamic data not available for this vehicle.",
            )
        raise

