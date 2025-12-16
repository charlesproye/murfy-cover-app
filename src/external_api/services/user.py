from datetime import timedelta

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import UUID4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.cookie_auth import authenticate_user, create_tokens
from external_api.schemas.user import GlobalUser, User, UserLogin

reusable_oauth2 = OAuth2PasswordBearer(tokenUrl="token")


async def login_with_cookies(
    form_data: UserLogin = Depends(), db: AsyncSession | None = None
):
    """Login method that sets secure httpOnly cookies"""
    data = await authenticate_user(form_data.email, form_data.password, db)
    if not data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(days=7)
    tokens = create_tokens(
        data={"sub": data["user"]["email"]}, access_expires_delta=access_token_expires
    )

    # Return user and company data without tokens (tokens will be in cookies)
    return {
        "user": data["user"],
        "company": data["company"],
        "tokens": tokens,
    }


async def get_user_by_id(db: AsyncSession, user_id: UUID4) -> GlobalUser | None:
    """
    Get a user by their ID.

    Args:
        db: Asynchronous database session
        user_id: ID of the user to retrieve

    Returns:
        The found user or None
    """
    query = text("""
    SELECT id, email, first_name, last_name, phone, company_id, role_id,
           password, last_connection
    FROM "User"
    WHERE id = :user_id
    """)

    async with db as session:
        result = await session.execute(query, {"user_id": user_id})
        user_data = result.fetchone()

        if not user_data:
            return None

        # Convert result to User schema, setting is_active to True by default
        user_dict = {
            "id": user_data.id,
            "email": user_data.email,
            "first_name": user_data.first_name,
            "last_name": user_data.last_name,
            "phone": user_data.phone,
            "company_id": user_data.company_id,
            "role_id": user_data.role_id,
            "is_active": True,  # By default, consider the user active
            "last_connection": user_data.last_connection,
        }

        return User(**user_dict)
