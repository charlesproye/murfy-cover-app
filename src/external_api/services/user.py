import logging
from datetime import timedelta

from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import UUID4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.security import (
    create_access_token,
    get_user_with_company_with_email,
    verify_password,
)
from external_api.schemas.user import Company, GlobalUser, Login

logger = logging.getLogger(__name__)

reusable_oauth2 = OAuth2PasswordBearer(tokenUrl="token")


class UserCrud:
    async def login(self, form_data: Login, db: AsyncSession | None = None):
        data = await authenticate_user(form_data.email, form_data.password, db)
        if not data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token_expires = timedelta(days=7)
        tokens = create_access_token(
            data={"sub": data["user"]["email"]},
            access_expires_delta=access_token_expires,
        )
        return {
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "token_type": "bearer",
            "user": data["user"],
            "company": data["company"],
        }


async def get_user_by_email(db: AsyncSession, email: str) -> tuple | None:
    """
    Récupère un utilisateur par son email.

    Args:
        db: Session de base de données asynchrone
        email: Email de l'utilisateur à récupérer

    Returns:
        Un tuple contenant les données brutes de l'utilisateur et l'objet utilisateur, ou (None, None)
    """
    query = text("""
    SELECT id, email, first_name, last_name, phone, company_id, role_id,
           password, last_connection
    FROM "user"
    WHERE email = :email
    """)

    async with db as session:
        result = await session.execute(query, {"email": email})
        user_data = result.fetchone()

        if not user_data:
            return None, None

        # Convertir le résultat en schéma User, en définissant is_active à True par défaut
        user_dict = {
            "id": user_data.id,
            "email": user_data.email,
            "first_name": user_data.first_name,
            "last_name": user_data.last_name,
            "phone": user_data.phone,
            "company_id": user_data.company_id,
            "role_id": user_data.role_id,
            "is_active": True,  # Par défaut, considérons l'utilisateur actif
            "last_connection": user_data.last_connection,
        }

        return user_data, GlobalUser(**user_dict)


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
    FROM "user"
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


async def authenticate_user(
    email: str, password: str, db: AsyncSession
) -> tuple[GlobalUser, Company]:
    """
    Authentifie un utilisateur.

    Args:
        db: Session de base de données asynchrone
        email: Email de l'utilisateur
        password: Mot de passe en clair

    Returns:
        Un tuple (user, company) ou (None, None) si l'authentification échoue
    """
    user, company = await get_user_with_company_with_email(email, db)
    if not user:
        logger.warning(
            f"Tentative d'authentification avec un email inexistant: {email}"
        )
        return None, None

    # Vérifier le mot de passe en utilisant les données brutes
    if not verify_password(password, user["password"]):
        logger.warning(
            f"Tentative d'authentification avec un mot de passe incorrect pour l'email: {email}"
        )
        return None, None

    logger.info(f"Utilisateur authentifié avec succès: {email}")
    return user, company


# async def get_current_active_user(
#     db: AsyncSession,
#     token: str
# ) -> User:
#     """
#     Récupère l'utilisateur actuellement authentifié et actif.

#     Args:
#         db: Session de base de données asynchrone
#         token: Token JWT d'authentification

#     Returns:
#         L'utilisateur authentifié et actif

#     Raises:
#         HTTPException: Si le token est invalide ou l'utilisateur n'existe pas
#     """
#     # Récupérer l'utilisateur à partir du token
#     user = await get_current_user(db=db, token=token)

#     # Puisque nous considérons tous les utilisateurs comme actifs, nous pouvons simplement retourner l'utilisateur
#     return user


async def get_user_with_fleet(email: str, db: AsyncSession):
    query = text("""
        SELECT
            u.company_id,
            u.id,
            u.role_id,
            u.email,
            json_agg(json_build_object('id', f.id, 'name', f.fleet_name)) as fleet_ids
        FROM "user" u
        LEFT JOIN "user_fleet" uf ON u.id = uf.user_id
        LEFT JOIN "fleet" f ON uf.fleet_id = f.id
        WHERE u.email = :email
        GROUP BY u.company_id, u.id, u.role_id, u.email
    """)
    async with db as session:
        result = await session.execute(query, {"email": email})
        if result:
            user_data = result.mappings().first()
            if user_data:
                user_data = dict(user_data)
                # Si fleet_ids est None ou contient un seul élément null, initialiser avec une liste vide
                if user_data["fleet_ids"] is None or (
                    len(user_data["fleet_ids"]) == 1
                    and user_data["fleet_ids"][0] is None
                ):
                    user_data["fleet_ids"] = []
                return user_data
    return None

