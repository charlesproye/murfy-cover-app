import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis, from_url

from external_api.core.config import settings

logger = logging.getLogger(__name__)

# Instance Redis
_redis_client: Redis | None = None


async def get_redis_client() -> Redis:
    """
    Crée et retourne un client Redis singleton.
    """
    global _redis_client
    if _redis_client is None:
        redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

        # Ajouter les informations d'authentification si présentes
        if settings.REDIS_USER and settings.REDIS_PASSWORD:
            redis_url = f"redis://{settings.REDIS_USER}:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"
        elif settings.REDIS_PASSWORD:
            redis_url = f"redis://:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/0"

        try:
            _redis_client = await from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            logger.info(
                f"Connexion établie à Redis sur {settings.REDIS_HOST}:{settings.REDIS_PORT}"
            )
        except Exception as e:
            logger.error(f"Échec de la connexion à Redis: {e!s}")
            raise

    return _redis_client


async def increment_and_check_rate_limit(
    user_id: uuid.UUID, limit: int, expiry_seconds: int = 86400
) -> tuple[int, bool]:
    """
    Incrémente un compteur de taux et vérifie si la limite est dépassée.

    Args:
        user_id: ID de l'utilisateur
        limit: Limite maximale
        expiry_seconds: Durée de vie du compteur en secondes (par défaut 24h)

    Returns:
        Tuple contenant (compteur actuel, booléen indiquant si la limite est dépassée)
    """
    try:
        redis = await get_redis_client()

        # Clé Redis pour suivre les appels d'API
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"rate_limit:{user_id}:day:{today}"

        # Récupérer la valeur actuelle
        current_value = await redis.get(key)

        if current_value is None:
            # Clé n'existe pas, l'initialiser à 1
            await redis.set(key, 1, ex=expiry_seconds)
            return 1, False

        # Convertir en entier
        current_count = int(current_value)

        # Vérifier si la limite est déjà dépassée
        if current_count >= limit:
            return current_count, True

        # Incrémenter le compteur
        new_count = await redis.incr(key)

        # Rafraichir l'expiration
        await redis.expire(key, expiry_seconds)

        return new_count, new_count > limit
    except Exception as e:
        logger.error(f"Erreur Redis lors du contrôle de limite: {e!s}")
        # En cas d'erreur Redis, permettre l'accès (fail-open pour éviter le blocage des utilisateurs)
        return 0, False


async def add_distinct_vin_and_check_limit(
    user_id: uuid.UUID, vin: str, max_distinct_vins: int, expiry_seconds: int = 86400
) -> tuple[int, bool]:
    """
    Ajoute un VIN au set des VINs distincts pour l'utilisateur et vérifie la limite.

    Args:
        user_id: ID de l'utilisateur
        vin: VIN à ajouter
        max_distinct_vins: Nombre maximum de VINs distincts autorisés
        expiry_seconds: Durée de vie du set en secondes (par défaut 24h)

    Returns:
        Tuple contenant (nombre de VINs distincts, booléen indiquant si la limite est dépassée)
    """
    try:
        redis = await get_redis_client()

        # Clé pour le set des VINs distincts de l'utilisateur pour aujourd'hui
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"distinct_vins:{user_id}:{today}"

        # Ajouter le VIN au set (SADD retourne 1 si l'élément est nouveau, 0 sinon)
        is_new = await redis.sadd(key, vin)

        # Définir l'expiration si c'est un nouveau set
        if is_new == 1:
            pipe = redis.pipeline()
            await pipe.expire(key, expiry_seconds)
            await pipe.scard(key)  # Récupérer le nombre d'éléments
            result = await pipe.execute()
            distinct_count = result[1]
        else:
            # Juste récupérer le nombre d'éléments
            distinct_count = await redis.scard(key)

        # Vérifier si la limite est dépassée
        is_limit_exceeded = distinct_count > max_distinct_vins

        return distinct_count, is_limit_exceeded
    except Exception as e:
        logger.error(f"Erreur Redis lors de l'ajout de VIN: {e!s}")
        # En cas d'erreur Redis, permettre l'accès (fail-open)
        return 0, False


async def get_distinct_vin_count(user_id: uuid.UUID) -> int:
    """
    Obtient le nombre de VINs distincts consultés par l'utilisateur aujourd'hui.

    Args:
        user_id: ID de l'utilisateur

    Returns:
        Nombre de VINs distincts
    """
    try:
        redis = await get_redis_client()

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        key = f"distinct_vins:{user_id}:{today}"

        distinct_count = await redis.scard(key)
        return distinct_count
    except Exception as e:
        logger.error(f"Erreur Redis lors du comptage de VINs: {e!s}")
        return 0


async def get_distinct_vins(user_id: uuid.UUID) -> list[str]:
    """
    Obtient la liste des VINs distincts consultés par l'utilisateur aujourd'hui.

    Args:
        user_id: ID de l'utilisateur

    Returns:
        Liste des VINs distincts
    """
    redis = await get_redis_client()

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    key = f"distinct_vins:{user_id}:{today}"

    vins = await redis.smembers(key)
    return list(vins)


async def cache_vehicle_data(vin: str, data: dict[str, Any], ttl: int = 3600) -> None:
    """
    Cache vehicle data with expiration

    Args:
        vin: Vehicle Identification Number
        data: Vehicle data to cache
        ttl: Time to live in seconds (default: 1 hour)
    """
    redis = await get_redis_client()
    key = f"vehicle:{vin}"
    serialized = json.dumps(data)
    await redis.set(key, serialized, ex=ttl)


async def get_cached_vehicle_data(vin: str) -> dict[str, Any] | None:
    """
    Get cached vehicle data if exists

    Args:
        vin: Vehicle Identification Number

    Returns:
        Vehicle data if cached, None otherwise
    """
    redis = await get_redis_client()
    key = f"vehicle:{vin}"
    data = await redis.get(key)

    if data:
        return json.loads(data)
    return None

