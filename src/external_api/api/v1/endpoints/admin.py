"""Endpoints for API administration"""

import logging
import uuid
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from external_api.core.security import get_current_user
from external_api.db.session import get_db
from external_api.schemas.api import (
    ApiPricingPlanCreate,
    ApiPricingPlanUpdate,
    ApiUserPricingCreate,
    ApiUserPricingUpdate,
    ApiUserRead,
)
from external_api.schemas.api import ApiPricingPlanRead as ApiPricingPlan
from external_api.schemas.api import ApiUserPricingRead as ApiUserPricing
from external_api.schemas.api import ApiUserRead as ApiUser
from external_api.schemas.user import User

logger = logging.getLogger(__name__)

router = APIRouter()


# Vérification que l'utilisateur API est un admin
async def get_admin_api_user(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
) -> ApiUser:
    """Vérifie que l'utilisateur API a les droits d'administration"""
    # Récupérer l'utilisateur interne associé
    query = select(User).where(User.id == user.user_id)
    result = await db.execute(query)
    user = result.scalars().first()

    # Vérifier si l'utilisateur a un rôle admin
    if not user or user.role_id != uuid.UUID(
        "00000000-0000-0000-0000-000000000001"
    ):  # ID supposé du rôle admin
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Droits d'administration requis",
        )

    return user


@router.get("/api-users", response_model=list[ApiUserRead])
async def get_api_users(
    skip: int = 0,
    limit: int = 100,
    is_active: bool | None = None,
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Récupère la liste des utilisateurs API.
    Accessible uniquement aux administrateurs.
    """
    query = select(ApiUser).join(User)

    if is_active is not None:
        query = query.filter(ApiUser.is_active == is_active)

    query = query.offset(skip).limit(limit)
    result = await db.execute(query)
    api_users = result.scalars().all()

    return api_users


@router.get("/api-users/{api_user_id}", response_model=ApiUserRead)
async def get_api_user(
    api_user_id: uuid.UUID = Path(..., description="ID de l'utilisateur API"),
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Récupère un utilisateur API spécifique.
    Accessible uniquement aux administrateurs.
    """
    query = select(ApiUser).where(ApiUser.id == api_user_id)
    result = await db.execute(query)
    api_user = result.scalars().first()

    if not api_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Utilisateur API non trouvé",
        )

    return api_user


@router.put("/api-users/{api_user_id}/active", response_model=ApiUserRead)
async def update_api_user_active_status(
    api_user_id: uuid.UUID = Path(..., description="ID de l'utilisateur API"),
    is_active: bool = Query(..., description="Nouvel état d'activation"),
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Active ou désactive un utilisateur API.
    Accessible uniquement aux administrateurs.
    """
    query = select(ApiUser).where(ApiUser.id == api_user_id)
    result = await db.execute(query)
    api_user = result.scalars().first()

    if not api_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Utilisateur API non trouvé",
        )

    api_user.is_active = is_active
    await db.commit()
    await db.refresh(api_user)

    action = "activé" if is_active else "désactivé"
    logger.info(f"Utilisateur API {api_user_id} {action}")

    return api_user


# Gestion des plans tarifaires
@router.get("/pricing-plans", response_model=list[ApiPricingPlan])
async def get_pricing_plans(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Récupère la liste des plans tarifaires.
    Accessible uniquement aux administrateurs.
    """
    query = select(ApiPricingPlan).offset(skip).limit(limit)
    result = await db.execute(query)
    plans = result.scalars().all()

    return plans


@router.post(
    "/pricing-plans", response_model=ApiPricingPlan, status_code=status.HTTP_201_CREATED
)
async def create_pricing_plan(
    plan: ApiPricingPlanCreate,
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Crée un nouveau plan tarifaire.
    Accessible uniquement aux administrateurs.
    """
    # Vérifier si un plan avec ce nom existe déjà
    query = select(ApiPricingPlan).where(ApiPricingPlan.name == plan.name)
    result = await db.execute(query)
    existing_plan = result.scalars().first()

    if existing_plan:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Un plan tarifaire avec le nom '{plan.name}' existe déjà",
        )

    # Créer le nouveau plan
    new_plan = ApiPricingPlan(
        name=plan.name,
        description=plan.description,
        requests_limit=plan.requests_limit,
        max_distinct_vins=plan.max_distinct_vins,
        price_per_request=plan.price_per_request,
    )

    db.add(new_plan)
    await db.commit()
    await db.refresh(new_plan)

    logger.info(f"Nouveau plan tarifaire créé: {plan.name}")

    return new_plan


@router.put("/pricing-plans/{plan_id}", response_model=ApiPricingPlan)
async def update_pricing_plan(
    plan_id: uuid.UUID = Path(..., description="ID du plan tarifaire"),
    plan_update: ApiPricingPlanUpdate = None,
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Met à jour un plan tarifaire existant.
    Accessible uniquement aux administrateurs.
    """
    # Récupérer le plan existant
    query = select(ApiPricingPlan).where(ApiPricingPlan.id == plan_id)
    result = await db.execute(query)
    existing_plan = result.scalars().first()

    if not existing_plan:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Plan tarifaire non trouvé",
        )

    # Mettre à jour les champs si fournis
    if plan_update.name is not None:
        # Vérifier si le nouveau nom n'est pas déjà utilisé
        if plan_update.name != existing_plan.name:
            name_query = select(ApiPricingPlan).where(
                ApiPricingPlan.name == plan_update.name
            )
            name_result = await db.execute(name_query)
            name_exists = name_result.scalars().first()

            if name_exists:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Un plan tarifaire avec le nom '{plan_update.name}' existe déjà",
                )

        existing_plan.name = plan_update.name

    if plan_update.description is not None:
        existing_plan.description = plan_update.description

    if plan_update.requests_limit is not None:
        existing_plan.requests_limit = plan_update.requests_limit

    if plan_update.max_distinct_vins is not None:
        existing_plan.max_distinct_vins = plan_update.max_distinct_vins

    if plan_update.price_per_request is not None:
        existing_plan.price_per_request = plan_update.price_per_request

    await db.commit()
    await db.refresh(existing_plan)

    logger.info(f"Plan tarifaire {plan_id} mis à jour")

    return existing_plan


# Gestion des associations utilisateur-plan
@router.get("/api-users/{api_user_id}/pricing", response_model=ApiUserPricing)
async def get_user_pricing(
    api_user_id: uuid.UUID = Path(..., description="ID de l'utilisateur API"),
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Récupère l'association tarifaire actuelle d'un utilisateur API.
    Accessible uniquement aux administrateurs.
    """
    today = date.today()

    # Récupérer l'association tarifaire active
    query = (
        select(ApiUserPricing)
        .options(joinedload(ApiUserPricing.pricing_plan))
        .where(
            and_(
                ApiUserPricing.api_user_id == api_user_id,
                ApiUserPricing.effective_date <= today,
                (ApiUserPricing.expiration_date is None)
                | (ApiUserPricing.expiration_date >= today),
            )
        )
        .order_by(desc(ApiUserPricing.effective_date))
    )

    result = await db.execute(query)
    user_pricing = result.scalars().first()

    if not user_pricing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Association tarifaire non trouvée pour cet utilisateur",
        )

    return user_pricing


@router.post(
    "/api-users/{api_user_id}/pricing",
    response_model=ApiUserPricing,
    status_code=status.HTTP_201_CREATED,
)
async def create_user_pricing(
    api_user_id: uuid.UUID = Path(..., description="ID de l'utilisateur API"),
    pricing: ApiUserPricingCreate = None,
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Crée une nouvelle association tarifaire pour un utilisateur API.
    Accessible uniquement aux administrateurs.
    """
    # Vérifier que l'utilisateur API existe
    user_query = select(ApiUser).where(ApiUser.id == api_user_id)
    user_result = await db.execute(user_query)
    api_user = user_result.scalars().first()

    if not api_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Utilisateur API non trouvé",
        )

    # Vérifier que le plan tarifaire existe
    plan_query = select(ApiPricingPlan).where(
        ApiPricingPlan.id == pricing.pricing_plan_id
    )
    plan_result = await db.execute(plan_query)
    pricing_plan = plan_result.scalars().first()

    if not pricing_plan:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Plan tarifaire non trouvé",
        )

    # Créer l'association
    new_user_pricing = ApiUserPricing(
        api_user_id=api_user_id,
        pricing_plan_id=pricing.pricing_plan_id,
        custom_requests_limit=pricing.custom_requests_limit,
        custom_max_distinct_vins=pricing.custom_max_distinct_vins,
        custom_price_per_request=pricing.custom_price_per_request,
        effective_date=pricing.effective_date,
        expiration_date=pricing.expiration_date,
    )

    db.add(new_user_pricing)
    await db.commit()
    await db.refresh(new_user_pricing)

    logger.info(
        f"Nouvelle association tarifaire créée pour l'utilisateur API {api_user_id}"
    )

    return new_user_pricing


@router.put("/api-users/{api_user_id}/pricing/current", response_model=ApiUserPricing)
async def update_current_user_pricing(
    api_user_id: uuid.UUID = Path(..., description="ID de l'utilisateur API"),
    pricing_update: ApiUserPricingUpdate = None,
    db: AsyncSession = Depends(get_db),
    _: ApiUser = Depends(get_admin_api_user),
) -> Any:
    """
    Met à jour l'association tarifaire actuelle d'un utilisateur API.
    Accessible uniquement aux administrateurs.
    """
    today = date.today()

    # Récupérer l'association tarifaire active
    query = (
        select(ApiUserPricing)
        .where(
            and_(
                ApiUserPricing.api_user_id == api_user_id,
                ApiUserPricing.effective_date <= today,
                (ApiUserPricing.expiration_date is None)
                | (ApiUserPricing.expiration_date >= today),
            )
        )
        .order_by(desc(ApiUserPricing.effective_date))
    )

    result = await db.execute(query)
    user_pricing = result.scalars().first()

    if not user_pricing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Association tarifaire non trouvée pour cet utilisateur",
        )

    # Mettre à jour les champs si fournis
    if pricing_update.pricing_plan_id is not None:
        # Vérifier que le nouveau plan existe
        plan_query = select(ApiPricingPlan).where(
            ApiPricingPlan.id == pricing_update.pricing_plan_id
        )
        plan_result = await db.execute(plan_query)
        pricing_plan = plan_result.scalars().first()

        if not pricing_plan:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Plan tarifaire non trouvé",
            )

        user_pricing.pricing_plan_id = pricing_update.pricing_plan_id

    if pricing_update.custom_requests_limit is not None:
        user_pricing.custom_requests_limit = pricing_update.custom_requests_limit

    if pricing_update.custom_max_distinct_vins is not None:
        user_pricing.custom_max_distinct_vins = pricing_update.custom_max_distinct_vins

    if pricing_update.custom_price_per_request is not None:
        user_pricing.custom_price_per_request = pricing_update.custom_price_per_request

    if pricing_update.effective_date is not None:
        user_pricing.effective_date = pricing_update.effective_date

    if pricing_update.expiration_date is not None:
        user_pricing.expiration_date = pricing_update.expiration_date

    await db.commit()
    await db.refresh(user_pricing)

    logger.info(
        f"Association tarifaire mise à jour pour l'utilisateur API {api_user_id}"
    )

    return user_pricing

