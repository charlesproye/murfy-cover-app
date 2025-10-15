"""Endpoints for model data access"""

import logging
import time

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.cookie_auth import get_current_user_from_cookie
from external_api.db.session import get_db
from external_api.schemas.api import ApiUserRead as ApiUser
from external_api.services.api_pricing import get_api_user_pricing, log_api_call
from external_api.services.redis import (
    add_distinct_vin_and_check_limit,
    increment_and_check_rate_limit,
)

logger = logging.getLogger(__name__)

router = APIRouter()


async def check_rate_limit(
    vin: str,
    endpoint: str,
    api_user: ApiUser = Depends(get_current_user_from_cookie()),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Checks rate limits and logs API calls.
    Checks two limits:
    1. Total number of requests per day
    2. Number of distinct VINs per day
    """
    start_time = time.time()

    # Get pricing plan
    pricing_info = await get_api_user_pricing(db, api_user.id)

    # 1. Check total requests limit
    current_count, limit_exceeded = await increment_and_check_rate_limit(
        api_user.id, pricing_info["requests_limit"]
    )

    if limit_exceeded:
        logger.warning(
            f"Request limit exceeded for API user {api_user.id}, endpoint {endpoint}"
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Daily request limit exceeded. Please upgrade your plan or try again tomorrow.",
        )

    # 2. Check distinct VINs limit
    distinct_count, vin_limit_exceeded = await add_distinct_vin_and_check_limit(
        api_user.id, vin, pricing_info["max_distinct_vins"]
    )

    if vin_limit_exceeded:
        logger.warning(f"Distinct VINs limit exceeded for API user {api_user.id}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Distinct VINs limit exceeded ({pricing_info['max_distinct_vins']}). Please upgrade your plan or try again tomorrow.",
        )

    # Calculate response time
    response_time = (time.time() - start_time) * 1000  # in milliseconds

    # Log this API call
    await log_api_call(db, api_user.id, endpoint, vin, response_time=response_time)

    logger.info(
        f"API Call logged: API user {api_user.id}, endpoint {endpoint}, "
        f"VIN {vin}, requests {current_count}/{pricing_info['requests_limit']}, "
        f"distinct VINs {distinct_count}/{pricing_info['max_distinct_vins']}"
    )

