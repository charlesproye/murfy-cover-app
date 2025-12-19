"""Endpoints for vehicle data access"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.core.utils import check_rate_limit
from external_api.db.session import get_db
from external_api.schemas.user import GetCurrentUser
from external_api.schemas.vehicle import (
    DynamicVehicleData,
    VehicleEligibilityResponse,
)
from external_api.services.vehicle import (
    check_vehicle_eligibility,
    get_dynamic_vehicle_data,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/check-eligibility/{vin}", response_model=VehicleEligibilityResponse)
async def check_vehicle_eligibility_endpoint(
    vin: str = Path(
        ..., description="VIN of the vehicle to check", min_length=10, max_length=25
    ),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
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
        ) from e


@router.get("/{vin}", response_model=DynamicVehicleData)
async def get_dynamic_vehicle(
    vin: str = Path(
        ..., description="VIN of the vehicle", min_length=10, max_length=25
    ),
    db: AsyncSession = Depends(get_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
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
        ) from e

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
            ) from e
