"""Schemas for external API related models"""

from datetime import date, datetime
from enum import Enum

from pydantic import UUID4, BaseModel, Field


class ApiPricingPlanEnum(str, Enum):
    """Enumération des plans de tarification disponibles"""

    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class ApiUserBase(BaseModel):
    """Base schema for API users"""

    api_key: str = Field(..., description="Clé API unique pour l'utilisateur")
    is_active: bool = Field(
        True, description="Indique si l'accès API de l'utilisateur est actif"
    )


class ApiUserCreate(ApiUserBase):
    """Schema for creating a new API user"""

    user_id: UUID4 = Field(..., description="ID de l'utilisateur interne lié")


class ApiUserRead(ApiUserBase):
    """Schema for reading API user data"""

    id: UUID4 = Field(..., description="ID unique de l'utilisateur API")
    user_id: UUID4 = Field(..., description="ID de l'utilisateur interne lié")
    created_at: datetime = Field(..., description="Date de création")
    last_access: datetime | None = Field(None, description="Dernière date d'accès")

    class Config:
        from_attributes = True


class ApiPricingPlanBase(BaseModel):
    """Schéma de base pour un plan de tarification API"""

    name: str
    description: str | None = None
    requests_limit: int
    max_distinct_vins: int
    price_per_request: float


class ApiPricingPlanCreate(ApiPricingPlanBase):
    """Schema for creating a new pricing plan"""


class ApiPricingPlanUpdate(BaseModel):
    """Schema for updating a pricing plan"""

    name: str | None = Field(None, description="Nom du plan")
    description: str | None = Field(None, description="Description du plan")
    requests_limit: int | None = Field(
        None, description="Limite quotidienne de requêtes", ge=0
    )
    max_distinct_vins: int | None = Field(
        None, description="Nombre maximal de VINs distincts autorisés par jour", ge=0
    )
    price_per_request: float | None = Field(
        None, description="Prix par requête en euros", ge=0
    )


class ApiPricingPlanRead(ApiPricingPlanBase):
    """Schéma pour la lecture d'un plan de tarification"""

    id: UUID4
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ApiUserPricingBase(BaseModel):
    """Schéma de base pour les tarifications utilisateur API"""

    user_id: UUID4
    pricing_plan_id: UUID4
    custom_requests_limit: int | None = None
    custom_max_distinct_vins: int | None = None
    custom_price_per_request: float | None = None
    effective_date: date
    expiration_date: date | None = None


class ApiUserPricingCreate(ApiUserPricingBase):
    """Schema for creating a user pricing association"""


class ApiUserPricingUpdate(BaseModel):
    """Schema for updating a user pricing association"""

    pricing_plan_id: UUID4 | None = Field(
        None, description="ID du plan de tarification"
    )
    custom_requests_limit: int | None = Field(
        None, description="Limite personnalisée de requêtes", ge=0
    )
    custom_max_distinct_vins: int | None = Field(
        None, description="Limite personnalisée de VINs distincts", ge=0
    )
    custom_price_per_request: float | None = Field(
        None, description="Prix personnalisé par requête", ge=0
    )
    effective_date: date | None = Field(None, description="Date d'entrée en vigueur")
    expiration_date: date | None = Field(
        None, description="Date d'expiration si applicable"
    )


class ApiUserPricingRead(ApiUserPricingBase):
    """Schéma pour la lecture d'une tarification utilisateur"""

    id: UUID4
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ApiUserBillingInfo(BaseModel):
    """Schema for API user billing information"""

    user_id: UUID4 = Field(..., description="User ID")
    email: str = Field(..., description="User email")
    plan_name: str = Field(..., description="Pricing plan name")
    plan_description: str | None = Field(None, description="Pricing plan description")
    requests_limit: int = Field(..., description="Daily request limit")
    max_distinct_vins: int = Field(
        ..., description="Maximum number of distinct VINs allowed per day"
    )
    current_distinct_vins: int = Field(
        ..., description="Current number of distinct VINs used today"
    )
    price_per_request: float = Field(..., description="Price per request in euros")
    unbilled_calls: int = Field(..., description="Number of unbilled calls")
    total_cost: float = Field(..., description="Total cost to be billed")
    effective_date: date = Field(..., description="Pricing plan effective date")
    expiration_date: date | None = Field(
        None, description="Pricing plan expiration date"
    )

    class Config:
        from_attributes = True


class ApiCallLogBase(BaseModel):
    """Schéma de base pour un journal d'appel API"""

    api_user_id: UUID4
    vin: str
    endpoint: str
    timestamp: datetime
    response_time: float | None = None
    status_code: int | None = None
    is_billed: bool = False
    billed_at: datetime | None = None


class ApiCallLogRead(ApiCallLogBase):
    """Schéma pour la lecture d'un journal d'appel"""

    id: UUID4

    class Config:
        from_attributes = True


class ApiBillingBase(BaseModel):
    """Schéma de base pour une facturation API"""

    api_user_id: UUID4
    period_start: date
    period_end: date
    total_requests: int = 0
    distinct_vins: int = 0
    total_amount: float = 0
    invoice_number: str | None = None
    paid: bool = False
    payment_date: datetime | None = None


class ApiBillingRead(ApiBillingBase):
    """Schéma pour la lecture d'une facturation"""

    id: UUID4
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

