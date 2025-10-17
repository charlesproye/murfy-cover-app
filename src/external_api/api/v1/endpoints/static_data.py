"""Endpoints for vehicle data access"""

import logging
import time
from typing import Any

import fastapi
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db_models.vehicle import VehicleModel
from external_api.core import utils
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.db.session import get_db
from external_api.schemas.model import ModelWarrantyData
from external_api.schemas.static_data import ModelTrendline, ModelType, SOHWithTrendline
from external_api.schemas.user import GetCurrentUser
from external_api.services.api_pricing import get_api_user_pricing, log_api_call
from external_api.services.redis import (
    add_distinct_vin_and_check_limit,
    increment_and_check_rate_limit,
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
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
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


# LS TODO: VinDecoderResponse is not defined, seems like this function never been used
# def decode_vin(vin) -> dict:
#     """
#     Decode a VIN and return the make, model, and year
#     """
#     api_prefix = "https://api.vindecoder.eu/3.2"
#     api_key = settings.VIN_DECODER_API_KEY
#     secret_key = settings.VIN_DECODER_SECRET_KEY
#     id = "decode"

#     # Calculate control sum (first 10 characters of SHA1 hash)
#     control_sum = hashlib.sha1(
#         (vin.upper() + "|" + id + "|" + api_key + "|" + secret_key).encode("utf-8")
#     ).hexdigest()[:10]

#     # Build the URL
#     url = (
#         api_prefix
#         + "/"
#         + api_key
#         + "/"
#         + control_sum
#         + "/"
#         + id
#         + "/"
#         + vin.upper()
#         + ".json"
#     )

#     # Make the request
#     response = requests.get(url)

#     # Parse JSON response
#     if response.status_code == 200:
#         result = response.json()
#         vehicle_data = result.get("decode", {})

#         return VinDecoderResponse(
#             vin=vin,
#             make=vehicle_data.get("make"),
#             model=vehicle_data.get("model"),
#             version=vehicle_data.get("version"),
#             year=vehicle_data.get("year"),
#             body_type=vehicle_data.get("body_type"),
#             engine_type=vehicle_data.get("engine_type"),
#             transmission=vehicle_data.get("transmission"),
#             fuel_type=vehicle_data.get("fuel_type"),
#         )
#         return result
#     else:
#         raise Exception(f"API request failed with status code: {response.status_code}")


# LS TODO: vin is not defined (x2), seems like this function never been used
# async def check_model_eligibility(db: AsyncSession, model: str) -> dict:
#     """
#     Check if we have some data on this model

#     Args:
#         db: Session de base de données asynchrone
#         model: name of the model

#     Returns:
#         Dictionnaire contenant les informations sur l'existence, l'éligibilité et l'activation du véhicule
#     """
#     logger.info(f"Vérification de l'éligibilité pour le VIN {model}")

#     try:
#         # Vérifier si le véhicule existe et récupérer son statut d'éligibilité et d'activation
#         query = text("""
#         SELECT
#             v.id,
#             v.is_eligible,
#             v.activation_status
#         FROM vehicle v
#         WHERE v.vin = :vin
#         """)

#         result = await db.execute(query, {"vin": vin})
#         record = result.fetchone()

#         if not record:
#             # Le véhicule n'existe pas dans la base de données
#             return {"exists": False, "is_eligible": False, "is_activated": False}

#         # Le véhicule existe, vérifier son éligibilité et son statut d'activation
#         return {
#             "exists": True,
#             "is_eligible": bool(record.is_eligible),
#             "is_activated": bool(record.activation_status),
#         }

#     except Exception as e:
#         logger.error(
#             f"Erreur lors de la vérification de l'éligibilité pour le VIN {vin}: {e!s}"
#         )
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Erreur lors de la vérification de l'éligibilité du véhicule",
#         ) from e


@router.get("/models-with-data", response_model=list[ModelType])
async def get_model_with_data(
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> Any:
    query = text("""
    SELECT
        m.make_name,
        vm.model_name,
        vm.type,
        vm.commissioning_date,
        vm.end_of_life_date
    FROM vehicle_model vm
    INNER JOIN make m ON vm.make_id = m.id
    INNER JOIN battery b ON vm.battery_id = b.id
    WHERE
        vm.trendline->>'trendline' is not null
        AND b.capacity is not null
    """)
    vehicules_query = await db.execute(query)
    vehicules = vehicules_query.fetchall()

    return [
        ModelType(
            model_name=vehicule.model_name,
            model_type=vehicule.type,
            commissioning_date=vehicule.commissioning_date,
            end_of_life_date=vehicule.end_of_life_date,
        )
        for vehicule in vehicules
    ]


@router.get("/{model}/trendline", response_model=ModelTrendline)
async def get_model_trendline(
    model: str = Path(..., description="Model name"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> ModelTrendline:
    """
    Get trendline data for a specific model.

    Args:
        model: Name of the model
        user: Current authenticated user
        db: Database session

    Returns:
        ModelTrendline containing the trendline information
    """
    try:
        query = text("""
        SELECT
            vm.id,
            vm.model_name,
            vm.trendline->>'trendline' as trendline_mean,
            vm.trendline_min->>'trendline' as trendline_min,
            vm.trendline_max->>'trendline' as trendline_max,
            vm.commissioning_date,
            vm.end_of_life_date,
            vm.version
        FROM vehicle_model vm
        WHERE vm.model_name = :model_name and vm.trendline is not null
        """)
        result = await db.execute(query, {"model_name": model})
        record = result.fetchone()

        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Model {model} not found"
            )

        # Convert the record to a dictionary and ensure all fields are present

        if record.version:
            comment = "This trendline is based on the version of the model"
        else:
            comment = "No comment"

        return ModelTrendline(
            model_name=record.model_name,
            trendline_mean=record.trendline_mean,
            trendline_min=record.trendline_min,
            trendline_max=record.trendline_max,
            commissioning_date=record.commissioning_date,
            end_of_life_date=record.end_of_life_date,
            comment=comment,
        )

    except Exception as e:
        logger.error(f"Error retrieving trendline data for model {model}: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving model data",
        ) from e


@router.get("/{model}/warranty", response_model=ModelWarrantyData)
async def get_model_warranty(
    model: str = Path(..., description="Model name"),
    _: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get warranty data for a specific model.

    Args:
        model: Name of the model
        user: Current authenticated user
        db: Database session

    Returns:
        ModelWarrantyData containing the warranty information
    """
    try:
        query = text("""
        SELECT
            vm.id,
            vm.model_name,
            vm.warranty_km,
            vm.warranty_date
        FROM vehicle_model vm
        WHERE vm.model_name = :model_name
        """)
        result = await db.execute(query, {"model_name": model})
        record = result.fetchone()

        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Model {model} not found"
            )

        # Convert the record to a dictionary and ensure all fields are present
        data = {
            "id": record.id,
            "model_name": record.model_name,
            "warranty_km": record.warranty_km,
            "warranty_date": record.warranty_date,
        }

        return ModelWarrantyData(**data)

    except Exception as e:
        logger.error(f"Error retrieving warranty data for model {model}: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving model data",
        ) from e


@router.get("/{vin}/flash-soh", response_model=SOHWithTrendline)
async def get_model_soh_trendline(
    request: fastapi.Request,
    vin: str | None = Path(..., description="VIN of the vehicle"),
    model: str = Query(..., description="Model name"),
    odometer: int = Query(..., ge=0, description="Odometer in km"),
    model_type: str = Query(..., description="Type of the model"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> SOHWithTrendline:
    """
    Get SOH data for a specific model + model_type for a given odometer.

    Args:
        model: Name of the model
        model_type: Type of the model
        odometer: Odometer in km
        user: Current authenticated user
        db: Database session

    Returns:
        SOHWithTrendline containing the SOH information
    """
    try:
        await check_rate_limit(
            vin=vin, endpoint=request.scope["route"].path, user=user, db=db
        )

        query = select(
            VehicleModel.trendline["trendline"].as_string().label("trendline"),
            VehicleModel.trendline_min["trendline"].as_string().label("trendline_min"),
            VehicleModel.trendline_max["trendline"].as_string().label("trendline_max"),
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
        ).where(
            VehicleModel.model_name == model,
            VehicleModel.type == model_type,
            VehicleModel.trendline.is_not(None),
        )

        trendline_result = await db.execute(query)
        trendline = trendline_result.fetchone()

        if not trendline:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model {model} + model_type {model_type} not found",
            )

        soh = round(utils.numpy_safe_eval(trendline.trendline, x=odometer), 2)

        return SOHWithTrendline(
            trendline_mean=trendline.trendline,
            soh=soh,
            odometer=odometer,
            trendline_min=trendline.trendline_min,
            trendline_max=trendline.trendline_max,
            commissioning_date=trendline.commissioning_date,
            end_of_life_date=trendline.end_of_life_date,
        )

    except Exception as e:
        logger.exception(
            f"Error retrieving soh data for model {model} + model_type {model_type}: {e!s}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving model data",
        ) from e

