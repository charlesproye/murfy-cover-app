"""Models for api-related tables"""

import uuid

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDModel


class ApiUser(BaseUUIDModel):
    __tablename__ = "api_user"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    api_key: str = Column(String(100), unique=True, nullable=False)
    is_active: bool = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    last_access = Column(DateTime)

    # Index pour les recherches rapides
    __table_args__ = (
        Index("ix_api_user_user_id", "user_id"),
        Index("ix_api_user_api_key", "api_key"),
    )


class ApiPricingPlan(BaseUUIDModel):
    __tablename__ = "api_pricing_plan"
    name: str = Column(String(50), nullable=False, unique=True)
    description: str = Column(String)
    requests_limit: int = Column(
        Integer, nullable=False, comment="Limite quotidienne de requêtes API"
    )
    max_distinct_vins: int = Column(
        Integer,
        nullable=False,
        comment="Nombre maximal de VINs distincts autorisés par jour",
    )
    price_per_request: float = Column(
        Numeric(10, 4), nullable=False, comment="Prix par requête en euros"
    )
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ApiUserPricing(BaseUUIDModel):
    __tablename__ = "api_user_pricing"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=True)
    pricing_plan_id: Mapped[uuid.UUID] = Column(
        ForeignKey("api_pricing_plan.id"), nullable=False
    )
    custom_requests_limit: int = Column(
        Integer, comment="Limite personnalisée qui remplace celle du plan si définie"
    )
    custom_max_distinct_vins: int = Column(
        Integer,
        comment="Limite de VINs distincts personnalisée qui remplace celle du plan si définie",
    )
    custom_price_per_request: float = Column(
        Numeric(10, 4), comment="Prix personnalisé qui remplace celui du plan si défini"
    )
    effective_date = Column(
        Date,
        nullable=False,
        comment="Date d'entrée en vigueur de ce plan pour cet utilisateur",
    )
    expiration_date = Column(Date, comment="Date d'expiration si applicable")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Index pour les recherches rapides
    __table_args__ = (
        Index("ix_user_pricing_user_id", "user_id"),
        Index("ix_user_pricing_pricing_plan_id", "pricing_plan_id"),
    )


class ApiCallLog(BaseUUIDModel):
    __tablename__ = "api_call_log"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    vin: str = Column(String(50), nullable=False)
    endpoint: str = Column(
        String(100),
        nullable=False,
        comment="Point d'accès appelé (ex: /vehicle/static)",
    )
    timestamp = Column(DateTime, server_default=func.now(), nullable=False)
    response_time = Column(Float, comment="Temps de réponse en millisecondes")
    status_code = Column(Integer, comment="Code de statut HTTP de la réponse")
    is_billed: bool = Column(Boolean, default=False, nullable=False)
    billed_at = Column(DateTime)

    # Index pour les recherches rapides et l'efficacité des requêtes
    __table_args__ = (
        Index("ix_api_call_log_user_id", "user_id"),
        Index("ix_api_call_log_vin", "vin"),
        Index("ix_api_call_log_timestamp", "timestamp"),
        Index("ix_api_call_log_is_billed", "is_billed"),
    )


class ApiBilling(BaseUUIDModel):
    __tablename__ = "api_billing"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    total_requests = Column(Integer, nullable=False, default=0)
    distinct_vins = Column(Integer, nullable=False, default=0)
    total_amount = Column(Numeric(10, 2), nullable=False, default=0)
    invoice_number: str = Column(String(50))
    paid: bool = Column(Boolean, default=False)
    payment_date = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Index pour les performances
    __table_args__ = (
        Index("ix_api_billing_user_id", "user_id"),
        Index("ix_api_billing_period", "period_start", "period_end"),
        Index("ix_api_billing_paid", "paid"),
    )
