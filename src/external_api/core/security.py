"""Security utilities for the API"""

from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.config import settings
from external_api.db.session import get_db
from external_api.schemas.user import GlobalUser, UserWithFleet

# Configuration du contexte de cryptage
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
reusable_oauth2 = OAuth2PasswordBearer(tokenUrl="auth/token")

# Configuration JWT
ALGORITHM = settings.ALGORITHM
SECRET_KEY = settings.SECRET_KEY


def create_access_token(
    subject: str | Any, expires_delta: timedelta | None = None
) -> str:
    """
    Create JWT access token

    Args:
        subject: Token subject (usually user ID)
        expires_delta: Token expiration time

    Returns:
        Encoded JWT token
    """
    print("=== CRÉATION TOKEN ===")
    print(f"Sujet du token: {subject}")

    if expires_delta:
        expire = datetime.now(UTC) + expires_delta
    else:
        expire = datetime.now(UTC) + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    print(f"Expiration: {expire}")

    to_encode: dict[str, Any] = {
        "exp": expire,
        "sub": str(subject),
        "iat": datetime.now(UTC),
    }
    print(f"Payload: {to_encode}")

    # Ensure SECRET_KEY is properly encoded
    secret_key = (
        settings.SECRET_KEY.encode("utf-8")
        if isinstance(settings.SECRET_KEY, str)
        else settings.SECRET_KEY
    )
    print(f"SECRET_KEY type: {type(secret_key)}")

    try:
        encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
        print(f"Token créé avec succès: {encoded_jwt[:10]}...")
        print("=== FIN CRÉATION TOKEN ===")
        return encoded_jwt
    except Exception as e:
        print(f"ERREUR création token: {e!s}")
        raise


async def refresh_token(refresh_token: str, db: AsyncSession):
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
        new_tokens = create_access_token(data={"sub": email})
        return new_tokens
    except JWTError:
        return None


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify password against hash

    Args:
        plain_password: Plain text password
        hashed_password: Hashed password

    Returns:
        True if password matches hash
    """
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception as e:
        # Log l'erreur mais continue l'exécution
        import logging

        logging.getLogger(__name__).warning(
            f"Erreur lors de la vérification du mot de passe: {e!s}"
        )
        # On utilise une méthode de secours pour vérifier si l'avertissement est lié à bcrypt.__about__
        try:
            # Vérifier directement avec bcrypt pour contourner l'erreur de passlib
            password_bytes = plain_password.encode("utf-8")
            hashed_bytes = (
                hashed_password.encode("utf-8")
                if isinstance(hashed_password, str)
                else hashed_password
            )
            return bcrypt.checkpw(password_bytes, hashed_bytes)
        except Exception as inner_e:
            logging.getLogger(__name__).error(
                f"Erreur de secours lors de la vérification du mot de passe: {inner_e!s}"
            )
            # En dernier recours, on utilise la méthode initiale
            return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    Hash password

    Args:
        password: Plain text password

    Returns:
        Hashed password
    """
    return pwd_context.hash(password)


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


async def get_current_user(
    token: str = Depends(reusable_oauth2), db: AsyncSession = Depends(get_db)
) -> GlobalUser:
    """
    Get the current authenticated user.

    Args:
        token: JWT token
        db: Database session

    Returns:
        The authenticated user

    Raises:
        HTTPException: If the token is invalid or the user doesn't exist
    """
    print("=== DÉBUT AUTHENTIFICATION ===")
    print(f"Token reçu: {token[:10]}...")

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        print("Tentative de décodage du token...")
        # Decode token and verify expiration
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_exp": True}
        )
        print(f"Token décodé avec succès: {payload}")

        email: str = payload.get("sub")
        if not email:
            print("ERREUR: Pas d'email dans le token")
            raise credentials_exception

        print(f"Email trouvé dans le token: {email}")

        user = await get_user(email, db)
        if user is None:
            print(f"ERREUR: Aucun utilisateur trouvé pour l'email: {email}")
            raise credentials_exception

        print(f"Utilisateur trouvé: {user.get('email')}")
        sanitized_user = sanitize_user(user)
        print("=== FIN AUTHENTIFICATION SUCCÈS ===")
        return GlobalUser(**sanitized_user)

    except JWTError as e:
        print(f"ERREUR JWT: {e!s}")
        raise credentials_exception
    except Exception as e:
        print(f"ERREUR INATTENDUE: {e!s}")
        raise credentials_exception


async def get_user_with_company_with_email(email: str, db: AsyncSession):
    query = text("""
        SELECT
            "user".*,
            "company".name AS company_name,
            "company".description AS company_description,
            "role".role_name AS role,
            fleet.id AS fleet_id,
            fleet.fleet_name
        FROM
            "user"
        LEFT JOIN "company" ON "user".company_id = "company".id
        LEFT JOIN "role" ON "user".role_id = "role".id
        LEFT JOIN "user_fleet" ON "user".id = "user_fleet".user_id
        LEFT JOIN "fleet" ON "user_fleet".fleet_id = "fleet".id
        WHERE "user".email = :email
    """)

    async with db as session:
        result = await session.execute(query, {"email": email})
        rows = result.mappings().all()

        if not rows:
            return None, None

    # Extract user data (excluding company and fleet fields)
    user = dict(rows[0].items())

    # Extract company data
    company = {
        key.replace("company_", ""): value
        for key, value in rows[0].items()
        if key.startswith("company_")
    }

    user["fleet_ids"] = [
        {"id": row["fleet_id"], "name": row["fleet_name"]}
        for row in rows
        if row["fleet_id"] is not None
    ]
    # Récupérer les IDs des flot
    return user, company


async def get_user_with_fleet_id(email: str, fleet_id: str, db: AsyncSession):
    query = text("""
        SELECT
            u.company_id,
            u.id,
            u.role_id,
            u.email,
            uf.fleet_id
        FROM "user" u
        LEFT JOIN "user_fleet" uf ON u.id = uf.user_id
        WHERE u.email = :email AND uf.fleet_id = :fleet_id
    """)
    result = await db.exec(query, {"email": email, "fleet_id": fleet_id})
    if result:
        return result.mappings().first()


async def get_user_with_fleet(email: str, db: AsyncSession):
    query = text("""
        SELECT
            u.company_id,
            u.id,
            u.role_id,
            u.email,
            json_agg(json_build_object('id', f.id, 'name', f.fleet_name)) as fleet_ids,
            (SELECT f2.id FROM "user_fleet" uf2
             JOIN "fleet" f2 ON uf2.fleet_id = f2.id
             WHERE uf2.user_id = u.id
             LIMIT 1) as fleet_id
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
            # Si fleet_ids est None ou contient un seul élément null, initialiser avec une liste vide
            if user_data["fleet_ids"] is None or (
                len(user_data["fleet_ids"]) == 1 and user_data["fleet_ids"][0] is None
            ):
                user_data["fleet_ids"] = []
            return user_data
    return None


def get_current_user_with_fleet():
    async def get_current_user_with_fleet(
        token: str = Depends(reusable_oauth2), db: AsyncSession = Depends(get_db)
    ):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials 3",
            headers={"WWW-Authenticate": "Bearer"},
        )
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email: str = payload.get("sub")
        except JWTError:
            raise credentials_exception
        user = await get_user_with_fleet(email, db)
        if user is None:
            raise credentials_exception
        sanitized_user = sanitize_user(user)
        return UserWithFleet(**sanitized_user)

    return get_current_user_with_fleet


def get_current_user_with_fleet_id(fleet_id: str):
    async def get_current_user_with_fleet_id(
        token: str = Depends(reusable_oauth2), db: AsyncSession = Depends(get_db)
    ):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials 1",
            headers={"WWW-Authenticate": "Bearer"},
        )
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email: str = payload.get("sub")
        except JWTError:
            raise credentials_exception
        user = await get_user_with_fleet_id(email, fleet_id, db)
        if user is None:
            raise HTTPException(
                status_code=500,
                detail="You can't access to this fleet",
                headers={"WWW-Authenticate": "Bearer"},
            )
        sanitized_user = sanitize_user(user)
        return UserWithFleet(**sanitized_user)

    return get_current_user_with_fleet_id

