"""Schémas Pydantic pour la tarification"""

from datetime import datetime
from enum import Enum

from pydantic import UUID4, BaseModel, Field


class PricingPlanEnum(str, Enum):
    """Enumération des plans de tarification disponibles"""

    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"
    STANDARD = "Plan Standard"


class PricingPlanBase(BaseModel):
    """Schéma de base pour un plan de tarification"""

    name: str = Field(..., description="Nom du plan de tarification")
    description: str | None = Field(None, description="Description du plan")
    requests_limit: int = Field(
        ..., description="Limite quotidienne de requêtes API", ge=1
    )
    max_distinct_vins: int = Field(
        ..., description="Nombre maximal de VINs distincts autorisés par jour", ge=1
    )
    price_per_request: float = Field(..., description="Prix par requête en euros", ge=0)


class PricingPlanCreate(PricingPlanBase):
    """Schéma pour la création d'un plan de tarification"""


class PricingPlanUpdate(BaseModel):
    """Schéma pour la mise à jour d'un plan de tarification"""

    name: str | None = Field(None, description="Nom du plan")
    description: str | None = Field(None, description="Description du plan")
    requests_limit: int | None = Field(
        None, description="Limite quotidienne de requêtes", ge=1
    )
    max_distinct_vins: int | None = Field(
        None, description="Nombre maximal de VINs distincts autorisés par jour", ge=1
    )
    price_per_request: float | None = Field(
        None, description="Prix par requête en euros", ge=0
    )


class PricingPlan(PricingPlanBase):
    """Schéma pour un plan de tarification complet"""

    id: UUID4
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserBillingInfo(BaseModel):
    """Schéma pour les informations de facturation"""

    user_id: UUID4
    pricing_plan: PricingPlanEnum
    requests_limit: int
    max_distinct_vins: int
    price_per_request: float
    current_requests: int
    current_distinct_vins: int
    remaining_requests: int
    remaining_distinct_vins: int
    unbilled_calls: int = Field(0, description="Nombre d'appels API non facturés")

    class Config:
        from_attributes = True


class BillingPaymentCreate(BaseModel):
    """Schéma pour la création d'un paiement"""

    user_id: UUID4
    amount: float
    invoice_number: str | None = None

