import logging
from datetime import timedelta
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.config import settings
from external_api.core.security import (
    create_access_token,
    get_current_user,
)
from external_api.core.utils import CrudTesla
from external_api.db.session import get_db
from external_api.schemas.token import Token
from external_api.schemas.user import GlobalUser, Login, LoginResponse, User
from external_api.services.user import authenticate_user

router = APIRouter()

logger = logging.getLogger(__name__)


class LoginForm(BaseModel):
    """Formulaire de connexion utilisant l'email"""

    email: EmailStr
    password: str


@router.post("/login", response_model=LoginResponse, include_in_schema=False)
async def login(
    # form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
    body: Login = Body(...),
) -> Any:
    """
    OAuth2 compatible token login, get an access token for future requests.

    This endpoint is used by the OAuth2PasswordBearer for authentication.
    """
    user, company = await authenticate_user(body.email, body.password, db)
    if not user:
        logger.warning(f"Failed login attempt for email {body.email}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = timedelta(days=7)  # Refresh token lasts 7 days

    access_token = create_access_token(
        user["email"], expires_delta=access_token_expires
    )
    refresh_token = create_access_token(
        user["email"], expires_delta=refresh_token_expires
    )
    logger.info(f"Successful login for email {body.email}")
    response: LoginResponse = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "hello": "world",
        "token_type": "bearer",
        "user": user,
        "company": company,
    }
    return response


@router.post("/token", response_model=Token, include_in_schema=False)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)
) -> Any:
    """
    OAuth2 compatible token endpoint for Swagger UI authorization.
    """
    user, _company = await authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        user["email"], expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=User)
async def read_users_me(
    current_user: GlobalUser = Depends(get_current_user),
) -> Any:
    """
    Get the current user's information.
    """
    logger.info(f"Getting user information for {current_user.email}")
    return current_user


# @router.post("/login")
# async def login(
#     db = Depends(deps.get_db_by_schema),
#     body: Login = Body(...)
# ):
#     response: LoginResponse = await User().login(body, db)
#     return response


##### For personnal telsa vehicle, we need to get the bearer token to get the activation
@router.post("/bearer")
async def get_bearer(db=Depends(get_db), data: dict = Body(...)):
    await CrudTesla().insert_code_tesla(body=data, db_session=db)
    return {"status": "done"}

