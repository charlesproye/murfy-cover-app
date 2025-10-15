"""Endpoints for billing management"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.cookie_auth import get_current_user_from_cookie
from external_api.db.session import get_db
from external_api.schemas.api import ApiUserBillingInfo
from external_api.schemas.api import ApiUserRead as ApiUser
from external_api.schemas.pricing import UserBillingInfo
from external_api.schemas.user import User
from external_api.services.api_pricing import (
    get_user_billing_info as api_get_user_billing_info,
)
from external_api.services.pricing import get_user_billing_info

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/usage", response_model=UserBillingInfo)
async def get_billing_usage(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user_from_cookie),
) -> Any:
    """
    Get the current user's billing information.

    Returns:
    - The current billing plan information
    - The number of unpaid API calls
    - The total cost to be billed
    """
    logger.info(f"Getting billing information for user {user.email}")

    try:
        billing_info = await get_user_billing_info(db, user.id)
        return billing_info
    except Exception as e:
        logger.error(f"Error during billing information retrieval: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during billing information retrieval",
        ) from e


@router.get("/info", response_model=ApiUserBillingInfo)
async def get_billing_info(
    api_user: ApiUser = Depends(get_current_user_from_cookie),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Get the current API user's billing information.

    Returns the billing plan information, limits, current usage and costs.
    """
    logger.info(f"Getting billing information for API user {api_user.id}")

    try:
        # Get the billing information of the API user
        billing_info = await api_get_user_billing_info(db, api_user.id)
        return billing_info

    except Exception as e:
        logger.error(f"Error during billing information retrieval: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during billing information retrieval",
        ) from e

