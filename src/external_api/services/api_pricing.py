"""Service for API pricing and billing"""

import logging
import uuid
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.schemas.api import ApiUserBillingInfo
from external_api.schemas.api import ApiUserRead as ApiUser
from external_api.services.redis import get_distinct_vin_count

logger = logging.getLogger(__name__)


async def get_api_user_by_api_key(db: AsyncSession, api_key: str) -> ApiUser | None:
    """
    Récupère un utilisateur API par sa clé API.

    Args:
        db: Session de base de données
        api_key: Clé API à rechercher

    Returns:
        L'utilisateur API ou None si non trouvé
    """
    query = text("""
    SELECT u.id, u.email, u.first_name, u.last_name, u.api_key, u.is_active, u.last_access,
           u.created_at, u.updated_at
    FROM "user" u
    WHERE u.api_key = :api_key AND u.is_active = true
    """)

    result = await db.execute(query, {"api_key": api_key})
    user = result.fetchone()

    if user:
        # Mettre à jour le dernier accès
        update_query = text("""
        UPDATE "user"
        SET last_access = :last_access
        WHERE id = :id
        """)
        await db.execute(update_query, {"id": user.id, "last_access": datetime.now()})
        await db.commit()

    return user


async def get_api_user_pricing(db: AsyncSession, user_id: uuid.UUID) -> dict:
    """
    Obtient les informations de tarification pour un utilisateur.
    Prend en compte les limites personnalisées définies dans api_user_pricing si elles existent.

    Args:
        db: Session de base de données
        user_id: ID de l'utilisateur

    Returns:
        Dictionnaire contenant les informations de tarification (requests_limit, max_distinct_vins, price_per_request)
    """
    # Récupérer l'association utilisateur-plan la plus récente avec les éventuelles limites personnalisées
    association_query = text("""
    SELECT ap.id, u.id, ap.pricing_plan_id, ap.effective_date, ap.expiration_date,
           ap.custom_requests_limit, ap.custom_max_distinct_vins, ap.custom_price_per_request,
           p.requests_limit, p.max_distinct_vins, p.price_per_request
    FROM api_user_pricing ap
    JOIN "user" u ON ap.user_id = u.id
    JOIN api_pricing_plan p ON ap.pricing_plan_id = p.id
    WHERE ap.user_id = :user_id
      AND (ap.expiration_date IS NULL OR ap.expiration_date >= CURRENT_DATE)
    ORDER BY ap.effective_date DESC
    LIMIT 1
    """)

    result = await db.execute(association_query, {"user_id": user_id})
    pricing_data = result.fetchone()

    if not pricing_data:
        # Si aucun plan n'est trouvé, créer une association avec le plan par défaut
        logger.info(
            f"Aucun plan tarifaire trouvé pour l'utilisateur {user_id}, création d'un plan par défaut..."
        )

        # Vérifier si un plan standard existe
        plan_query = text("""
        SELECT id, requests_limit, max_distinct_vins, price_per_request
        FROM api_pricing_plan
        WHERE name = 'Plan Standard'
        """)

        result = await db.execute(plan_query)
        default_plan = result.fetchone()

        if not default_plan:
            # Créer un plan par défaut
            plan_id = uuid.uuid4()
            create_plan_query = text("""
            INSERT INTO api_pricing_plan (
                id, name, description, requests_limit, max_distinct_vins, price_per_request
            )
            VALUES (
                :id, 'Plan Standard', 'Plan standard pour tous les utilisateurs',
                1000, 100, 0.01
            )
            RETURNING id, requests_limit, max_distinct_vins, price_per_request
            """)

            result = await db.execute(create_plan_query, {"id": plan_id})
            default_plan = result.fetchone()

        # Associer l'utilisateur au plan par défaut
        association_id = uuid.uuid4()
        now = datetime.now()

        create_association_query = text("""
        INSERT INTO api_user_pricing (
            id, user_id, pricing_plan_id, effective_date, expiration_date
        )
        VALUES (
            :id, :user_id, :pricing_plan_id, :effective_date, NULL
        )
        """)

        await db.execute(
            create_association_query,
            {
                "id": association_id,
                "user_id": user_id,
                "pricing_plan_id": default_plan.id,
                "effective_date": now,
            },
        )
        await db.commit()

        # Retourner les valeurs du plan par défaut
        return {
            "requests_limit": default_plan.requests_limit,
            "max_distinct_vins": default_plan.max_distinct_vins,
            "price_per_request": default_plan.price_per_request,
        }

    # Préparer le dictionnaire de retour en utilisant les limites du plan
    pricing_info = {
        "requests_limit": pricing_data.requests_limit,
        "max_distinct_vins": pricing_data.max_distinct_vins,
        "price_per_request": pricing_data.price_per_request,
    }

    # Remplacer par les limites personnalisées si elles sont définies
    if pricing_data.custom_requests_limit is not None:
        pricing_info["requests_limit"] = pricing_data.custom_requests_limit
        logger.info(
            f"Utilisation d'une limite personnalisée de requêtes pour l'utilisateur {user_id}: {pricing_data.custom_requests_limit}"
        )

    if pricing_data.custom_max_distinct_vins is not None:
        pricing_info["max_distinct_vins"] = pricing_data.custom_max_distinct_vins
        logger.info(
            f"Utilisation d'une limite personnalisée de VINs pour l'utilisateur {user_id}: {pricing_data.custom_max_distinct_vins}"
        )

    if pricing_data.custom_price_per_request is not None:
        pricing_info["price_per_request"] = pricing_data.custom_price_per_request
        logger.info(
            f"Utilisation d'un prix personnalisé pour l'utilisateur {user_id}: {pricing_data.custom_price_per_request}"
        )

    return pricing_info


async def log_api_call(
    db: AsyncSession,
    user_id: uuid.UUID,
    endpoint: str,
    vin: str,
    status_code: int = 200,
    response_time: float | None = None,
) -> dict:
    """
    Enregistre un appel d'API dans la base de données.

    Args:
        db: Session de base de données
        user_id: ID de l'utilisateur
        endpoint: Point d'accès appelé
        vin: VIN du véhicule consulté
        status_code: Code de statut HTTP de la réponse
        response_time: Temps de réponse en millisecondes

    Returns:
        Dictionnaire contenant les informations de l'appel API enregistré
    """
    # Générer un UUID pour l'enregistrement
    call_id = uuid.uuid4()
    timestamp = datetime.now()

    # Insérer l'appel d'API dans la base de données
    insert_query = text("""
    INSERT INTO api_call_log (
        id, user_id, vin, endpoint, status_code,
        response_time, timestamp, is_billed
    )
    VALUES (
        :id, :user_id, :vin, :endpoint, :status_code,
        :response_time, :timestamp, false
    )
    RETURNING id, user_id, vin, endpoint, status_code, response_time, timestamp, is_billed
    """)

    try:
        result = await db.execute(
            insert_query,
            {
                "id": call_id,
                "user_id": user_id,
                "vin": vin,
                "endpoint": endpoint,
                "status_code": status_code,
                "response_time": response_time,
                "timestamp": timestamp,
            },
        )
        await db.commit()
        api_call_data = result.fetchone()

        # Log l'appel pour le débogage
        logger.info(
            f"API Call: {user_id} | {endpoint} | {vin} | {status_code} | {response_time}ms"
        )

        # Retourner les données de l'appel
        return {
            "id": api_call_data.id,
            "user_id": api_call_data.user_id,
            "vin": api_call_data.vin,
            "endpoint": api_call_data.endpoint,
            "status_code": api_call_data.status_code,
            "response_time": api_call_data.response_time,
            "timestamp": api_call_data.timestamp,
            "is_billed": api_call_data.is_billed,
        }
    except Exception as e:
        await db.rollback()
        logger.error(f"Erreur lors de l'enregistrement de l'appel API: {e!s}")

        # En cas d'erreur, retourner quand même un dictionnaire avec les informations
        return {
            "id": call_id,
            "user_id": user_id,
            "vin": vin,
            "endpoint": endpoint,
            "status_code": status_code,
            "response_time": response_time,
            "timestamp": timestamp,
            "is_billed": False,
            "error": str(e),
        }


async def get_user_billing_info(
    db: AsyncSession, user_id: uuid.UUID
) -> ApiUserBillingInfo:
    """
    Get billing information for an API user.

    Args:
        db: Database session
        user_id: User ID

    Returns:
        Billing information for the user
    """
    # Get information about the user
    user_query = text("""
    SELECT u.id, u.email, u.first_name, u.last_name
    FROM "user" u
    WHERE u.id = :user_id
    """)

    result = await db.execute(user_query, {"user_id": user_id})
    user = result.fetchone()

    if not user:
        raise ValueError(f"User {user_id} not found")

    # Get pricing information
    pricing_info = await get_api_user_pricing(db, user_id)

    # Get plan information
    pricing_association_query = text("""
    SELECT ap.id, ap.user_id, ap.pricing_plan_id, ap.effective_date, ap.expiration_date,
           p.name, p.description
    FROM api_user_pricing ap
    JOIN api_pricing_plan p ON ap.pricing_plan_id = p.id
    WHERE ap.user_id = :user_id
    ORDER BY ap.effective_date DESC
    LIMIT 1
    """)

    result = await db.execute(pricing_association_query, {"user_id": user_id})
    pricing_association = result.fetchone()

    if not pricing_association:
        raise ValueError(f"Pricing association not found for user {user_id}")

    # Number of distinct VINs used today
    current_distinct_vins = await get_distinct_vin_count(user_id)

    # Count unbilled API calls
    unbilled_calls_query = text("""
    SELECT COUNT(*) as count
    FROM api_call_log
    WHERE user_id = :user_id AND is_billed = false
    """)

    result = await db.execute(unbilled_calls_query, {"user_id": user_id})
    unbilled_calls_count = result.fetchone().count

    # Calculer le coût total
    total_cost = round(unbilled_calls_count * pricing_info["price_per_request"], 2)

    return ApiUserBillingInfo(
        user_id=user_id,
        email=user.email,
        plan_name=pricing_association.name,
        plan_description=pricing_association.description,
        requests_limit=pricing_info["requests_limit"],
        max_distinct_vins=pricing_info["max_distinct_vins"],
        current_distinct_vins=current_distinct_vins,
        price_per_request=pricing_info["price_per_request"],
        unbilled_calls=unbilled_calls_count,
        total_cost=total_cost,
        effective_date=pricing_association.effective_date,
        expiration_date=pricing_association.expiration_date,
    )


async def set_custom_user_limits(
    db: AsyncSession,
    user_id: uuid.UUID,
    custom_requests_limit: int | None = None,
    custom_max_distinct_vins: int | None = None,
    custom_price_per_request: float | None = None,
) -> bool:
    """
    Définit des limites personnalisées pour un utilisateur dans sa relation avec son plan tarifaire.
    Ces limites personnalisées prennent la priorité sur les limites du plan tarifaire.

    Args:
        db: Session de base de données
        user_id: ID de l'utilisateur
        custom_requests_limit: Limite personnalisée du nombre de requêtes par jour (None pour utiliser la limite du plan)
        custom_max_distinct_vins: Limite personnalisée du nombre de VINs distincts par jour (None pour utiliser la limite du plan)
        custom_price_per_request: Prix personnalisé par requête (None pour utiliser le prix du plan)

    Returns:
        True si les limites ont été mises à jour avec succès, False sinon
    """
    try:
        # Vérifier s'il existe une association active pour cet utilisateur
        check_query = text("""
        SELECT id
        FROM api_user_pricing
        WHERE user_id = :user_id
          AND (expiration_date IS NULL OR expiration_date >= CURRENT_DATE)
        ORDER BY effective_date DESC
        LIMIT 1
        """)

        result = await db.execute(check_query, {"user_id": user_id})
        pricing_assoc = result.fetchone()

        if not pricing_assoc:
            logger.error(
                f"Aucune association de plan tarifaire trouvée pour l'utilisateur {user_id}"
            )
            return False

        # Construire la requête de mise à jour
        update_query = text("""
        UPDATE api_user_pricing
        SET custom_requests_limit = :custom_requests_limit,
            custom_max_distinct_vins = :custom_max_distinct_vins,
            custom_price_per_request = :custom_price_per_request,
            updated_at = :updated_at
        WHERE id = :id
        """)

        await db.execute(
            update_query,
            {
                "id": pricing_assoc.id,
                "custom_requests_limit": custom_requests_limit,
                "custom_max_distinct_vins": custom_max_distinct_vins,
                "custom_price_per_request": custom_price_per_request,
                "updated_at": datetime.now(),
            },
        )
        await db.commit()

        logger.info(
            f"Limites personnalisées mises à jour pour l'utilisateur {user_id}: "
            f"requests={custom_requests_limit}, vins={custom_max_distinct_vins}, "
            f"price={custom_price_per_request}"
        )
        return True

    except Exception as e:
        await db.rollback()
        logger.error(f"Erreur lors de la mise à jour des limites personnalisées: {e!s}")
        return False

