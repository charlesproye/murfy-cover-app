import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, Response
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.cookie_auth import (
    CookieAuth,
    get_authenticated_user,
    get_current_user_with_refresh_from_cookie,
)
from external_api.core.utils import CrudTesla
from external_api.db.session import get_db
from external_api.schemas.user import LoginResponse, TokenResponse, UserLogin
from external_api.services.user import (
    login_with_cookies,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/login")
async def login(
    response: Response,
    db: AsyncSession = Depends(get_db),
    body: UserLogin = Body(...),
) -> Any:
    """Login endpoint that uses httpOnly cookies for security"""
    result = await login_with_cookies(form_data=body, db=db)

    # Set secure cookies
    CookieAuth.set_auth_cookies(
        response=response,
        access_token=result["tokens"]["access_token"],
        refresh_token=result["tokens"]["refresh_token"],
        user_data=result["user"],
        company_data=result["company"],
    )

    # Return user data without tokens
    return LoginResponse(user=result["user"], company=result["company"])


@router.post("/token")
async def get_token(
    db: AsyncSession = Depends(get_db),
    body: UserLogin = Body(...),
) -> Any:
    """Login endpoint that uses httpOnly cookies for security"""
    result = await login_with_cookies(form_data=body, db=db)

    # Return user data without tokens
    return TokenResponse(
        access_token=result["tokens"]["access_token"], token_type="bearer"
    )


@router.post("/logout")
async def logout(response: Response):
    """Logout endpoint that clears httpOnly cookies"""
    CookieAuth.clear_auth_cookies(response)
    return {"message": "Logged out successfully"}


@router.post("/refresh")
async def refresh_token(
    response: Response,
    tokens_and_emails: dict = Depends(get_current_user_with_refresh_from_cookie()),
    db=Depends(get_db),
):
    data = await get_authenticated_user(tokens_and_emails["email"], db)

    # Set secure cookies
    CookieAuth.set_auth_cookies(
        response=response,
        access_token=tokens_and_emails["access_token"],
        refresh_token=tokens_and_emails["refresh_token"],
        user_data=data["user"],
        company_data=data["company"],
    )
    return {"ok": True}


##### For personnal telsa vehicle, we need to get the bearer token to get the activation
@router.post("/bearer")
async def get_bearer(db=Depends(get_db), data: dict = Body(...)):
    await CrudTesla().insert_code_tesla(body=data, db_session=db)
    return {"status": "done"}

