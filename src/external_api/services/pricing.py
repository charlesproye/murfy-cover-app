import logging
import uuid
from datetime import datetime

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.schemas.pricing import PricingPlanEnum, UserBillingInfo
from external_api.services.redis import get_distinct_vin_count

logger = logging.getLogger(__name__)


async def get_user_pricing(db: AsyncSession, user_id: int) -> dict:
    """
    Get pricing information for a user

    Args:
        db: Database session
        user_id: User ID

    Returns:
        Dictionary containing pricing information if exists, None otherwise
    """
    try:
        # Étape 1 : Vérifier que l'utilisateur existe
        user_query = text("""
        SELECT id
        FROM "user"
        WHERE id = :user_id
        """)

        result = await db.execute(user_query, {"user_id": user_id})
        user = result.fetchone()

        if not user:
            logger.warning(f"Aucun utilisateur trouvé pour l'ID {user_id}")
            return None

        # Étape 2 : Obtenir les informations de pricing pour cet utilisateur
        query = text("""
        SELECT up.id, up.user_id, up.pricing_plan_id, up.custom_requests_limit,
               up.custom_max_distinct_vins, up.custom_price_per_request,
               up.effective_date, up.expiration_date
        FROM api_user_pricing up
        WHERE up.user_id = :user_id
        AND (up.expiration_date IS NULL OR up.expiration_date >= CURRENT_DATE)
        ORDER BY up.effective_date DESC
        LIMIT 1
        """)

        result = await db.execute(query, {"user_id": user_id})
        user_pricing = result.fetchone()

        if not user_pricing:
            logger.warning(f"Aucun plan tarifaire trouvé pour l'utilisateur {user_id}")
            return None

        return {
            "id": user_pricing.id,
            "user_id": user_pricing.user_id,
            "pricing_plan_id": user_pricing.pricing_plan_id,
            "custom_requests_limit": user_pricing.custom_requests_limit,
            "custom_max_distinct_vins": user_pricing.custom_max_distinct_vins,
            "custom_price_per_request": user_pricing.custom_price_per_request,
            "effective_date": user_pricing.effective_date,
            "expiration_date": user_pricing.expiration_date,
        }
    except Exception as e:
        logger.error(
            f"Erreur lors de la récupération des informations de tarification pour l'utilisateur {user_id}: {e!s}"
        )
        raise


async def log_api_call(db: AsyncSession, user_id: int, endpoint: str, vin: str) -> dict:
    """
    Enregistre un appel d'API dans le journal.

    Args:
        db: Session de base de données asynchrone
        user_id: ID de l'utilisateur
        endpoint: Point d'accès appelé
        vin: VIN du véhicule concerné

    Returns:
        Dictionnaire contenant les informations de l'appel API enregistré
    """
    logger.info(
        f"Enregistrement d'un appel API pour l'utilisateur {user_id}, endpoint {endpoint}, VIN {vin}"
    )

    try:
        # Obtenir l'ID d'utilisateur API à partir de l'ID utilisateur
        user_query = text("""
        SELECT id FROM "user" WHERE id = :user_id
        """)
        result = await db.execute(user_query, {"user_id": user_id})
        user = result.fetchone()

        if not user:
            logger.warning(f"Aucun utilisateur API trouvé pour l'utilisateur {user_id}")
            raise ValueError(f"Utilisateur API non trouvé pour l'utilisateur {user_id}")

        user_id = user.id

        # Générer un UUID pour l'enregistrement et un timestamp
        call_id = uuid.uuid4()
        timestamp = datetime.now()

        # Insérer l'appel API dans la table api_call_log
        insert_query = text("""
        INSERT INTO api_call_log (
            id, user_id, vin, endpoint, timestamp, response_time, status_code, is_billed
        ) VALUES (
            :id, :user_id, :vin, :endpoint, :timestamp, NULL, 200, false
        )
        RETURNING id, user_id, vin, endpoint, timestamp, response_time, status_code, is_billed
        """)

        result = await db.execute(
            insert_query,
            {
                "id": call_id,
                "user_id": user_id,
                "vin": vin,
                "endpoint": endpoint,
                "timestamp": timestamp,
            },
        )
        api_call_data = result.fetchone()

        await db.commit()

        # Retourner les données de l'appel
        return {
            "id": api_call_data.id,
            "user_id": api_call_data.user_id,
            "vin": api_call_data.vin,
            "endpoint": api_call_data.endpoint,
            "timestamp": api_call_data.timestamp,
            "response_time": api_call_data.response_time,
            "status_code": api_call_data.status_code,
            "is_billed": api_call_data.is_billed,
        }

    except Exception as e:
        logger.error(
            f"Erreur lors de l'enregistrement de l'appel API pour l'utilisateur {user_id}: {e!s}"
        )
        await db.rollback()
        raise


async def get_user_billing_info(db: AsyncSession, user_id: int) -> UserBillingInfo:
    """
    Get billing information for a user

    Args:
        db: Database session
        user_id: User ID

    Returns:
        UserBillingInfo object
    """
    try:
        # Get user pricing
        user_pricing = await get_user_pricing(db, user_id)
        if not user_pricing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User pricing not found",
            )

        # Get pricing plan
        pricing_plan_query = text("""
        SELECT pp.id, pp.name, pp.description, pp.requests_limit,
               pp.max_distinct_vins, pp.price_per_request
        FROM api_pricing_plan pp
        WHERE pp.id = :pricing_plan_id
        """)

        result = await db.execute(
            pricing_plan_query, {"pricing_plan_id": user_pricing["pricing_plan_id"]}
        )
        pricing_plan = result.fetchone()

        if not pricing_plan:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pricing plan not found",
            )

        # Get API user ID to use in call logs
        api_user_query = text("""
        SELECT id FROM "user" WHERE id = :user_id
        """)
        result = await db.execute(api_user_query, {"user_id": user_id})
        user = result.fetchone()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API user not found",
            )

        user_id = user.id

        # Count today's API calls
        today = datetime.now().date()
        api_calls_query = text("""
        SELECT COUNT(*) as count
        FROM api_call_log
        WHERE user_id = :user_id AND DATE(timestamp) = :today
        """)

        result = await db.execute(api_calls_query, {"user_id": user_id, "today": today})
        api_calls_today = result.fetchone().count

        # Get distinct VIN count from Redis
        current_distinct_vins = await get_distinct_vin_count(user_id)

        # Get effective limits
        requests_limit = (
            user_pricing["custom_requests_limit"] or pricing_plan.requests_limit
        )
        max_distinct_vins = (
            user_pricing["custom_max_distinct_vins"] or pricing_plan.max_distinct_vins
        )
        price_per_request = (
            user_pricing["custom_price_per_request"] or pricing_plan.price_per_request
        )

        # Count unbilled calls for the user
        unbilled_calls_query = text("""
        SELECT COUNT(*) as count
        FROM api_call_log
        WHERE user_id = :user_id AND is_billed = false
        """)

        result = await db.execute(unbilled_calls_query, {"user_id": user_id})
        unbilled_calls = result.fetchone().count

        # Convertir le nom du plan en valeur d'énumération
        try:
            plan_enum = PricingPlanEnum(pricing_plan.name)
        except ValueError:
            logger.warning(
                f"Plan '{pricing_plan.name}' non trouvé dans l'énumération PricingPlanEnum, utilisation de STANDARD par défaut"
            )
            plan_enum = PricingPlanEnum.STANDARD

        return UserBillingInfo(
            user_id=user_id,
            pricing_plan=plan_enum,
            requests_limit=requests_limit,
            max_distinct_vins=max_distinct_vins,
            price_per_request=price_per_request,
            current_requests=api_calls_today,
            current_distinct_vins=current_distinct_vins,
            remaining_requests=max(0, requests_limit - api_calls_today),
            remaining_distinct_vins=max(0, max_distinct_vins - current_distinct_vins),
            unbilled_calls=unbilled_calls,
        )
    except Exception as e:
        logger.error(
            f"Erreur lors de la récupération des informations de facturation pour l'utilisateur {user_id}: {e!s}"
        )
        raise

