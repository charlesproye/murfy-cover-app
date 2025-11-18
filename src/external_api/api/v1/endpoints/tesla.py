import logging
import urllib.parse
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiohttp
from fastapi import Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRouter
from fastapi.templating import Jinja2Templates
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import insert, select, update

from core.tesla import tesla_auth
from db_models.user_tokens import User, UserToken
from external_api.core.config import settings
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.core.http_client import HTTP_CLIENT
from external_api.core.utils import replace_in_url, strip_params_from_url
from external_api.db.session import get_db
from external_api.schemas.tesla import TeslaCreateUserRequest
from external_api.schemas.user import GetCurrentUser

LOGGER = logging.getLogger(__name__)

tesla_router = APIRouter()

# Setup templates directory
templates = Jinja2Templates(
    directory=str(Path(__file__).parent.parent.parent.parent / "templates")
)


# https://developer.tesla.com/docs/fleet-api/authentication/third-party-tokens
@tesla_router.get("/auth-callback", response_class=HTMLResponse)
async def tesla_auth_callback(
    request: Request,
    state: str = Query(...),
    code: str = Query(...),
    session: aiohttp.ClientSession = Depends(HTTP_CLIENT),
    db: AsyncSession = Depends(get_db),
):
    """
    Callback endpoint for Tesla authentication.
    This endpoint has to be open, as the the only one the end user will actually hit.
    """
    tesla_user_query = select(User).where(User.vin == state)
    tesla_user = (await db.execute(tesla_user_query)).scalar_one_or_none()

    if not tesla_user:
        return templates.TemplateResponse(
            "tesla_auth_callback.html",
            {
                "request": request,
                "status": "error",
                "error_type": "not_found",
                "title": "Vehicle Not Found",
                "message": f"No user associated with VIN: {state}",
                "details": "Please make sure you have registered your vehicle before attempting authentication.",
                "vin": state,
            },
        )

    try:
        await db.execute(
            insert(UserToken).values(
                user_id=tesla_user.id,
                code=code,
                callback_url=strip_params_from_url(request.url),
            )
        )
        await db.commit()
    except IntegrityError:
        LOGGER.warning(
            f"User code already generated and consumed for user {tesla_user.email} with vin={tesla_user.vin}"
        )
        return templates.TemplateResponse(
            "tesla_auth_callback.html",
            {
                "request": request,
                "status": "error",
                "error_type": "already_authenticated",
                "title": "Already Authenticated",
                "message": f"Authentication for VIN {tesla_user.vin} has already been completed.",
                "details": "This vehicle has already been authenticated. If you need to re-authenticate, please contact our support team.",
                "vin": tesla_user.vin,
            },
        )

    await refresh_token(user_id=tesla_user.id, session=session, db=db)

    return templates.TemplateResponse(
        "tesla_auth_callback.html",
        {
            "request": request,
            "status": "success",
            "vin": tesla_user.vin,
            "tesla_ak": "https://www.tesla.com/_ak/bib-data.fr",
        },
    )


@tesla_router.post("/create-user")
async def create_user(
    create_user_request: TeslaCreateUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
) -> str:
    """
    Creates a new Tesla user and returns the authentication URL.
    """
    call_back_url = replace_in_url(request.url, "/create-user", "/auth-callback")

    tesla_auth_params = {
        "client_id": settings.TESLA_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": str(call_back_url),
        "scope": "openid offline_access user_data vehicle_device_data energy_device_data",
        "state": create_user_request.vin,
    }
    tesla_auth_base = "https://auth.tesla.com/oauth2/v3/authorize"
    tesla_auth_url = f"{tesla_auth_base}?{urllib.parse.urlencode(tesla_auth_params)}"

    try:
        await db.execute(
            insert(User).values(
                vin=create_user_request.vin,
                email=create_user_request.email,
                region=create_user_request.region,
                full_name=create_user_request.name,
            )
        )
        await db.commit()
    except IntegrityError:
        await db.rollback()
        LOGGER.warning(
            f"User already exists for vin={create_user_request.vin}, updating email and region"
        )
        await db.execute(
            update(User)
            .where(User.vin == create_user_request.vin)
            .values(email=create_user_request.email, region=create_user_request.region)
        )
        await db.commit()

    return tesla_auth_url


@tesla_router.post("/refresh-token")
async def refresh_token(
    user_id: uuid.UUID,
    session: aiohttp.ClientSession = Depends(HTTP_CLIENT),
    db: AsyncSession = Depends(get_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
) -> dict[str, str]:
    if not all([settings.TESLA_CLIENT_ID, settings.TESLA_CLIENT_SECRET]):
        raise HTTPException(
            status_code=500, detail="Tesla credentials are not set on the server."
        )

    user_token_query = (
        select(UserToken)
        .where(UserToken.user_id == user_id)
        .order_by(UserToken.created_at.desc())
        .limit(1)
    )
    user_token: UserToken | None = (
        await db.execute(user_token_query)
    ).scalar_one_or_none()

    if user_token is None:
        raise HTTPException(
            status_code=404, detail=f"No user token associated with user={user_id}"
        )

    user_query = select(User).where(User.id == user_id)
    user = (await db.execute(user_query)).scalar_one_or_none()

    if user_token.refresh_token is None:
        tokens = await tesla_auth.tesla_tokens_from_code(
            session=session,
            code=user_token.code,
            client_id=settings.TESLA_CLIENT_ID,
            client_secret=settings.TESLA_CLIENT_SECRET,
            redirect_uri=user_token.callback_url,
            region=user.region,
        )
    else:
        tokens = await tesla_auth.refresh_token(
            session=session,
            refresh_token=user_token.refresh_token,
            client_id=settings.TESLA_CLIENT_ID,
        )

    user_token.access_token = tokens.access_token
    user_token.refresh_token = tokens.refresh_token
    user_token.expires_at = datetime.now(tz=UTC) + timedelta(seconds=tokens.expires_in)
    await db.commit()

    return {"message": f"Tokens refreshed successfully for user={user_id}"}
