"""
Secure cookie-based authentication utilities
Provides httpOnly cookie support for enhanced security
"""

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlmodel import text
from sqlmodel.ext.asyncio.session import AsyncSession

from external_api.core.config import settings
from external_api.db.session import get_db
from external_api.schemas.user import GetCurrentUser

# Configuration JWT
ALGORITHM = settings.ALGORITHM
SECRET_KEY = settings.SECRET_KEY

# Cookie configuration
COOKIE_NAME_ACCESS = "evalue_access_token"
COOKIE_NAME_REFRESH = "evalue_refresh_token"
COOKIE_NAME_SESSION = "evalue_session_data"

# Cookie settings
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "true").lower() == "true"
COOKIE_HTTPONLY = True
COOKIE_SAMESITE = "strict"
COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN", None)


def sanitize_user(user: Any) -> Any:
    user_dict = dict(user)  # Convertir l'objet en dictionnaire
    user_dict.pop("password", None)
    return user_dict


async def get_user(email: str, db: AsyncSession):
    query = text("""
        SELECT * FROM "user" WHERE email = :email
    """)
    result = await db.execute(query, {"email": email})
    return result.mappings().first()


class CookieAuth:
    """Handles secure cookie-based authentication"""

    @staticmethod
    def set_auth_cookies(
        response: Response,
        access_token: str,
        refresh_token: str,
        user_data: dict[str, Any],
        company_data: dict[str, Any],
        access_expires_delta: timedelta | None = None,
        refresh_expires_delta: timedelta | None = None,
    ) -> None:
        """Set secure authentication cookies"""

        # Calculate expiration times
        access_expire = datetime.now(UTC) + (access_expires_delta or timedelta(hours=1))
        refresh_expire = datetime.now(UTC) + (
            refresh_expires_delta or timedelta(days=30)
        )

        # Set access token cookie
        response.set_cookie(
            key=COOKIE_NAME_ACCESS,
            value=access_token,
            expires=access_expire,
            secure=COOKIE_SECURE,
            httponly=COOKIE_HTTPONLY,
            samesite=COOKIE_SAMESITE,
            domain=COOKIE_DOMAIN,
            path="/",
        )

        # Set refresh token cookie
        response.set_cookie(
            key=COOKIE_NAME_REFRESH,
            value=refresh_token,
            expires=refresh_expire,
            secure=COOKIE_SECURE,
            httponly=COOKIE_HTTPONLY,
            samesite=COOKIE_SAMESITE,
            domain=COOKIE_DOMAIN,
            path="/",
        )

        # Set session data cookie (non-sensitive user info only)
        session_data = {
            "user": {
                **user_data,
            },
            "company": {
                **company_data,
            },
        }

        # Encode session data as JWT for integrity
        session_token = jwt.encode(session_data, SECRET_KEY, algorithm=ALGORITHM)

        response.set_cookie(
            key=COOKIE_NAME_SESSION,
            value=session_token,
            expires=access_expire,  # Same as access token
            secure=COOKIE_SECURE,
            httponly=False,  # Allow client-side access for non-sensitive data
            samesite=COOKIE_SAMESITE,
            domain=COOKIE_DOMAIN,
            path="/",
        )

    @staticmethod
    def clear_auth_cookies(response: Response) -> None:
        """Clear all authentication cookies"""

        cookies_to_clear = [
            COOKIE_NAME_ACCESS,
            COOKIE_NAME_REFRESH,
            COOKIE_NAME_SESSION,
        ]

        for cookie_name in cookies_to_clear:
            response.delete_cookie(
                key=cookie_name,
                secure=COOKIE_SECURE,
                httponly=COOKIE_HTTPONLY
                if cookie_name != COOKIE_NAME_SESSION
                else False,
                samesite=COOKIE_SAMESITE,
                domain=COOKIE_DOMAIN,
                path="/",
            )

    @staticmethod
    def get_token_from_cookie(
        request: Request, token_type: str = "access"
    ) -> str | None:
        """Extract token from httpOnly cookie"""
        cookie_name = (
            COOKIE_NAME_ACCESS if token_type == "access" else COOKIE_NAME_REFRESH
        )
        return request.cookies.get(cookie_name)

    @staticmethod
    def get_session_data_from_cookie(request: Request) -> dict[str, Any] | None:
        """Extract and verify session data from cookie"""

        session_token = request.cookies.get(COOKIE_NAME_SESSION)
        if not session_token:
            return None

        try:
            payload = jwt.decode(session_token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except JWTError:
            return None


class CookieAuthBearer(HTTPBearer):
    """Custom HTTPBearer that supports both Authorization header and cookies"""

    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials | None:
        # First try to get token from Authorization header
        auth_header = await super().__call__(request)
        if auth_header:
            return auth_header

        # If no header, try to get from cookie
        token = CookieAuth.get_token_from_cookie(request, "access")
        if token:
            return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

        return None


# Create instance for dependency injection
cookie_auth_bearer = CookieAuthBearer(auto_error=False)

# Module-level dependency to avoid function call in defaults
_db_dependency = Depends(get_db)


def get_current_user_from_cookie(getUserFunction: Callable):
    """
    Dependency to get current user from cookie or header.

    This function now properly handles both:
    1. Authorization Bearer tokens from headers
    2. HttpOnly cookies as fallback

    Args:
        getUserFunction: Function to retrieve user from database
        security: HTTPBearer instance for token extraction
    """

    async def current_user(
        request: Request,
        db: AsyncSession = _db_dependency,
    ):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        token = None

        credentials = extract_bearer_token_from_request(request)
        if credentials and credentials.credentials:
            token = credentials.credentials
        else:
            token = CookieAuth.get_token_from_cookie(request, "access")

        if not token:
            raise credentials_exception

        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email: str = payload.get("sub")
            if email is None:
                raise credentials_exception
        except JWTError:
            raise credentials_exception from JWTError

        user = await getUserFunction(email, db)
        if user is None:
            raise credentials_exception

        return GetCurrentUser(**sanitize_user(user))

    return current_user


def get_current_user_with_refresh_from_cookie():
    """Dependency to refresh token using httpOnly cookies"""

    async def current_user_refresh(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = None,
        db: AsyncSession = _db_dependency,
    ):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials (refresh)",
            headers={"WWW-Authenticate": "Bearer"},
        )

        token = None

        # Try to get refresh token from credentials (header)
        if credentials:
            token = credentials.credentials
        else:
            # Try to get from cookie
            token = CookieAuth.get_token_from_cookie(request, "refresh")

        if not token:
            raise credentials_exception

        new_tokens_and_email = await get_new_tokens_and_email(token, db)
        if new_tokens_and_email is None:
            raise credentials_exception
        return new_tokens_and_email

    return current_user_refresh


async def get_user_with_fleet(email: str, db: AsyncSession):
    query = text("""
        SELECT
            u.company_id,
            u.id,
            u.role_id,
            u.email,
            json_agg(json_build_object('id', f.id, 'name', f.fleet_name) ORDER BY f.fleet_name) FILTER (WHERE f.id IS NOT NULL) as fleets
        FROM "user" u
        LEFT JOIN "user_fleet" uf ON u.id = uf.user_id
        LEFT JOIN "fleet" f ON uf.fleet_id = f.id
        WHERE u.email = :email
        GROUP BY u.company_id, u.id, u.role_id, u.email
    """)
    result = await db.execute(query, {"email": email})
    if result:
        user_data = result.mappings().first()
        if user_data:
            user_data = dict(user_data)
            # Si fleets est None ou contient un seul élément null, initialiser avec une liste vide
            if user_data["fleets"] is None or (
                len(user_data["fleets"]) == 1 and user_data["fleets"][0] is None
            ):
                user_data["fleets"] = []
            return user_data
    return None


### Helper functions
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(
        plain_password.encode("utf-8"), hashed_password.encode("utf-8")
    )


def create_tokens(
    data: dict,
    access_expires_delta: timedelta | None = None,
    refresh_expires_delta: timedelta | None = None,
):
    to_encode = data.copy()
    access_expire = datetime.now(UTC) + (access_expires_delta or timedelta(days=7))
    refresh_expire = datetime.now(UTC) + (refresh_expires_delta or timedelta(days=30))

    to_encode.update({"exp": access_expire})
    access_token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    to_encode.update({"exp": refresh_expire})
    refresh_token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return {"access_token": access_token, "refresh_token": refresh_token}


async def get_new_tokens_and_email(refresh_token: str, db: AsyncSession):
    try:
        # Decode the refresh token without verifying expiration
        payload = jwt.decode(
            refresh_token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"verify_exp": False},
        )
        email: str = payload.get("sub")

        # Check if the refresh token has expired
        exp = payload.get("exp")
        if exp and datetime.fromtimestamp(exp, tz=UTC) <= datetime.now(UTC):
            return None  # Refresh token has expired

        user = await get_user(email, db)
        if user is None:
            return None

        # Generate new access and refresh tokens
        new_tokens = create_tokens(data={"sub": email})
        return {**new_tokens, "email": email}
    except JWTError as e:
        print(f"JWT Error: {e!s}")
        return None


async def get_user_with_company_with_email(email: str, db: AsyncSession):
    query = text("""
        SELECT
            JSON_BUILD_OBJECT(
                'id', u.id,
                'first_name', u.first_name,
                'last_name', u.last_name,
                'password', u.password,
                'email', u.email,
                'phone', u.phone,
                'last_connection', u.last_connection,
                'is_active', u.is_active,
                'created_at', u.created_at,
                'updated_at', u.updated_at,
                'role', JSON_BUILD_OBJECT(
                    'id', r.id,
                    'name', r.role_name
                ),
                'fleets', COALESCE(
                    JSON_AGG(
                        CASE
                            WHEN f.id IS NOT NULL THEN
                                JSON_BUILD_OBJECT(
                                    'id', f.id,
                                    'name', f.fleet_name
                                )
                            ELSE NULL
                        END
                    ) FILTER (WHERE f.id IS NOT NULL),
                    '[]'::json
                )
            ) AS user,
            JSON_BUILD_OBJECT(
                'id', c.id,
                'name', c.name,
                'description', c.description
            ) AS company
        FROM
            "user" u
        LEFT JOIN "company" c ON u.company_id = c.id
        LEFT JOIN "role" r ON u.role_id = r.id
        LEFT JOIN "user_fleet" uf ON u.id = uf.user_id
        LEFT JOIN "fleet" f ON uf.fleet_id = f.id
        WHERE u.email = :email
        GROUP BY u.id, c.id, r.id
    """)
    result = await db.execute(query, {"email": email})
    row = result.mappings().first()
    user = row["user"]
    company = row["company"]
    return user, company


async def authenticate_user(email: str, password: str, db: AsyncSession):
    user, company = await get_user_with_company_with_email(email, db)
    if not user or not verify_password(password, user["password"]):
        return False
    sanitized_user = sanitize_user(user)
    return {"user": sanitized_user, "company": company}


async def get_authenticated_user(email: str, db: AsyncSession):
    user, company = await get_user_with_company_with_email(email, db)
    sanitized_user = sanitize_user(user)
    return {"user": sanitized_user, "company": company}


def extract_bearer_token_from_request(
    request: Request,
) -> HTTPAuthorizationCredentials | None:
    """
    Helper function to extract bearer token from request headers.
    Returns None if no bearer token is found.
    """
    try:
        # Try to extract bearer token from Authorization header
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token_value = auth_header[7:]  # Remove "Bearer " prefix
            return HTTPAuthorizationCredentials(
                scheme="Bearer", credentials=token_value
            )
    except Exception:
        return None

