# """Dependencies for FastAPI routes"""
# import logging
# from typing import Generator, Optional,AsyncGenerator, List
# import uuid
# from datetime import datetime

# from fastapi import Depends, HTTPException, Header, status
# from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
# from sqlalchemy.ext.asyncio import AsyncSession
# from starlette.requests import Request
# from sqlalchemy import text

# from external_api.db.session import get_db
# from external_api.schemas.api import ApiUserRead as ApiUser
# from external_api.schemas.user import User
# from external_api.services.api_pricing import get_api_user_by_api_key
# from external_api.core.security import get_current_user
# from db.session import get_db

# logger = logging.getLogger(__name__)

# API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# # Utilisation de HTTPBearer au lieu de OAuth2PasswordBearer pour simplifier l'interface Swagger
# security = HTTPBearer(auto_error=False)

# async def get_db_by_schema(schema: str = None) -> AsyncGenerator[AsyncSession, None]:
#     session = await get_db()
#     try:
#         yield session
#     finally:
#         await session.close()

# async def get_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> str:
#     """
#     Extrait le token Bearer du header d'autorisation pour l'utiliser dans l'authentification
#     """
#     if credentials is None:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token d'authentification manquant",
#             headers={"WWW-Authenticate": "Bearer"},
#         )
#     return credentials.credentials


# async def get_api_key(
#     x_api_key: Optional[str] = Header(None)
# ) -> str:
#     """
#     Récupère la clé API de l'en-tête.

#     Args:
#         x_api_key: Clé API fournie dans l'en-tête X-API-Key

#     Returns:
#         La clé API

#     Raises:
#         HTTPException: Si la clé API n'est pas fournie
#     """
#     if not x_api_key:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Clé API manquante. Veuillez fournir une clé API valide dans l'en-tête X-API-Key.",
#             headers={"WWW-Authenticate": "ApiKey"},
#         )
#     return x_api_key


# async def get_current_api_user_id(api_key: str = Depends(API_KEY_HEADER)) -> str:
#     """
#     Dépendance pour obtenir l'ID de l'utilisateur API à partir de la clé API.
#     La validation de la clé API est effectuée dans cette fonction.

#     Args:
#         api_key: Clé API fournie dans l'en-tête X-API-Key

#     Returns:
#         ID de l'utilisateur API si la clé est valide

#     Raises:
#         HTTPException: Si la clé API est manquante ou invalide
#     """
#     if not api_key:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Clé API manquante",
#             headers={"WWW-Authenticate": "ApiKey"},
#         )

#     # Ici, nous pourrions vérifier la clé API dans une base de données
#     # Pour cet exemple, nous acceptons toute clé non vide
#     # Dans un environnement de production, une vérification appropriée serait nécessaire

#     # Retourner un ID utilisateur fictif
#     return "12345"


# async def get_current_api_user(
#     token: str = Depends(get_token),
#     db: AsyncSession = Depends(get_db),
# ) -> ApiUser:
#     """
#     Dépendance pour obtenir l'utilisateur API à partir du token JWT.
#     Si l'utilisateur n'a pas encore de compte API, un compte est automatiquement créé.

#     Args:
#         token: Token JWT fourni dans l'en-tête Authorization
#         db: Session de base de données asynchrone

#     Returns:
#         Objet utilisateur API

#     Raises:
#         HTTPException: Si le token est manquant ou invalide
#     """
#     # Obtenir l'utilisateur normal à partir du token
#     user = await get_current_active_user(db=db, token=token)

#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token invalide ou expiré",
#             headers={"WWW-Authenticate": "Bearer"},
#         )

#     # Vérifier si l'utilisateur a déjà un compte API
#     api_user_query = text("""
#     SELECT id, user_id, api_key, is_active, created_at, last_access
#     FROM api_user
#     WHERE user_id = :user_id
#     """)

#     result = await db.execute(api_user_query, {"user_id": user.id})
#     api_user_data = result.fetchone()

#     if not api_user_data:
#         # Créer un nouveau compte API pour l'utilisateur
#         logger.info(f"Création d'un compte API pour l'utilisateur {user.email}")

#         # Générer une clé API unique
#         api_key = f"key_{uuid.uuid4().hex}"
#         api_user_id = uuid.uuid4()
#         now = datetime.now()

#         insert_query = text("""
#         INSERT INTO api_user (id, user_id, api_key, is_active, created_at, last_access)
#         VALUES (:id, :user_id, :api_key, :is_active, :created_at, :last_access)
#         RETURNING id, user_id, api_key, is_active, created_at, last_access
#         """)

#         try:
#             result = await db.execute(
#                 insert_query,
#                 {
#                     "id": api_user_id,
#                     "user_id": user.id,
#                     "api_key": api_key,
#                     "is_active": True,
#                     "created_at": now,
#                     "last_access": now
#                 }
#             )
#             await db.commit()
#             api_user_data = result.fetchone()

#             # Créer également une association avec le plan tarifaire par défaut
#             await create_default_pricing_plan(db, api_user_id)

#         except Exception as e:
#             await db.rollback()
#             logger.error(f"Erreur lors de la création du compte API: {str(e)}")
#             raise HTTPException(
#                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#                 detail="Erreur lors de la création du compte API"
#             )
#     else:
#         # Mettre à jour le dernier accès
#         update_query = text("""
#         UPDATE api_user
#         SET last_access = :last_access
#         WHERE id = :id
#         """)

#         await db.execute(
#             update_query,
#             {
#                 "id": api_user_data.id,
#                 "last_access": datetime.now()
#             }
#         )
#         await db.commit()

#     # Construire et retourner l'objet ApiUser
#     return ApiUser(
#         id=api_user_data.id,
#         user_id=api_user_data.user_id,
#         api_key=api_user_data.api_key,
#         is_active=api_user_data.is_active,
#         created_at=api_user_data.created_at,
#         last_access=api_user_data.last_access
#     )


# async def create_default_pricing_plan(db: AsyncSession, api_user_id: uuid.UUID) -> None:
#     """
#     Crée ou récupère le plan tarifaire par défaut et l'associe à l'utilisateur API.

#     Args:
#         db: Session de base de données
#         api_user_id: ID de l'utilisateur API

#     Raises:
#         HTTPException: En cas d'erreur lors de la création
#     """
#     # Vérifier si un plan par défaut existe déjà
#     plan_query = text("""
#     SELECT id, name, description, requests_limit, max_distinct_vins, price_per_request
#     FROM api_pricing_plan
#     WHERE name = 'Plan Standard'
#     """)

#     result = await db.execute(plan_query)
#     plan_data = result.fetchone()

#     if not plan_data:
#         # Créer un plan tarifaire par défaut
#         plan_id = uuid.uuid4()
#         default_plan_query = text("""
#         INSERT INTO api_pricing_plan (
#             id, name, description, requests_limit, max_distinct_vins, price_per_request
#         )
#         VALUES (
#             :id, :name, :description, :requests_limit, :max_distinct_vins, :price_per_request
#         )
#         RETURNING id
#         """)

#         try:
#             result = await db.execute(
#                 default_plan_query,
#                 {
#                     "id": plan_id,
#                     "name": "Plan Standard",
#                     "description": "Plan standard pour tous les utilisateurs",
#                     "requests_limit": 1000,
#                     "max_distinct_vins": 100,
#                     "price_per_request": 0.01
#                 }
#             )
#             plan_id_result = result.fetchone()
#             plan_id = plan_id_result.id
#         except Exception as e:
#             await db.rollback()
#             logger.error(f"Erreur lors de la création du plan tarifaire par défaut: {str(e)}")
#             raise HTTPException(
#                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#                 detail="Erreur lors de la création du plan tarifaire"
#             )
#     else:
#         plan_id = plan_data.id

#     # Associer l'utilisateur API au plan tarifaire
#     now = datetime.now()
#     association_id = uuid.uuid4()

#     association_query = text("""
#     INSERT INTO api_user_pricing (
#         id, api_user_id, pricing_plan_id, effective_date, expiration_date
#     )
#     VALUES (
#         :id, :api_user_id, :pricing_plan_id, :effective_date, :expiration_date
#     )
#     """)

#     try:
#         await db.execute(
#             association_query,
#             {
#                 "id": association_id,
#                 "api_user_id": api_user_id,
#                 "pricing_plan_id": plan_id,
#                 "effective_date": now,
#                 "expiration_date": None  # Sans date d'expiration
#             }
#         )
#         await db.commit()
#     except Exception as e:
#         await db.rollback()
#         logger.error(f"Erreur lors de l'association du plan tarifaire: {str(e)}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Erreur lors de l'association du plan tarifaire"
#         )


# async def get_current_admin_user(
#     request: Request,
#     db: AsyncSession = Depends(get_db),
# ) -> ApiUser:
#     """
#     Dépendance pour vérifier que l'utilisateur actuel est un administrateur.

#     Args:
#         request: Requête HTTP
#         db: Session de base de données asynchrone

#     Returns:
#         Objet utilisateur si l'utilisateur est un administrateur

#     Raises:
#         HTTPException: Si l'utilisateur n'est pas un administrateur
#     """
#     # Ici, vous devriez implémenter une logique pour vérifier les droits d'administration
#     # Par exemple, vérifier un jeton JWT, un cookie, etc.

#     # Pour cet exemple, nous supposons que l'utilisateur est un administrateur
#     # Dans un environnement de production, une vérification appropriée serait nécessaire

#     return ApiUser(id="admin-id", user_id="admin-user-id", api_key="admin-key", is_active=True)


# async def get_current_active_user_from_token(

#     token: str = Depends(get_token),
#     db: AsyncSession = Depends(get_db),
# ) -> User:
#     """
#     Dépendance pour obtenir l'utilisateur actuel à partir du token JWT.

#     Args:
#         token: Token JWT d'authentification
#         db: Session de base de données asynchrone

#     Returns:
#         L'utilisateur authentifié et actif

#     Raises:
#         HTTPException: Si le token est invalide ou l'utilisateur n'existe pas ou est inactif
#     """
#     user = await get_current_active_user(db=db, token=token)
#     return user

