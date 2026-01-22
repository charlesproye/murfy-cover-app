"""Useful functions for the API"""

import logging
import time
import uuid

from fastapi import Depends, HTTPException, status
from fastapi.datastructures import URL
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.sql_utils import get_async_db
from db_models import UserFleet
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.schemas.user import GetCurrentUser
from external_api.services.api_pricing import get_api_user_pricing, log_api_call
from external_api.services.redis import (
    add_distinct_vin_and_check_limit,
    increment_and_check_rate_limit,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def strip_params_from_url(url: URL) -> str:
    """
    Return the base URL without the query parameters.
    """
    return f"{url.scheme}://{url.netloc}{url.path}"


def replace_in_url(url: URL, old: str, new: str, with_params: bool = False) -> str:
    """
    Replace the old string in the URL with the new string.
    """

    new_path = url.path.replace(old, new)

    new_url = f"{url.scheme}://{url.netloc}{new_path}"

    if with_params:
        new_url += f"?{url.query}"

    return new_url


async def check_rate_limit(
    vin: str,
    endpoint: str,
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_async_db),
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
        logger.warning(
            f"Error during rate limit check: {e!s}. Continuing without limits."
        )
        response_time = (time.time() - start_time) * 1000
        logger.info(
            f"API Call logged (without limits): API user {user.id}, endpoint {endpoint}, "
            f"VIN {vin}"
        )


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
