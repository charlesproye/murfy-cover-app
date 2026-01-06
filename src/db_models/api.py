"""Models for api-related tables"""

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
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
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import BaseUUIDModel


class ApiUser(BaseUUIDModel):
    __tablename__ = "api_user"
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"), nullable=False)
    api_key: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    last_access: Mapped[datetime | None] = mapped_column(DateTime)

    # Index pour les recherches rapides
    __table_args__ = (
        Index("ix_api_user_user_id", "user_id"),
        Index("ix_api_user_api_key", "api_key"),
    )


class ApiPricingPlan(BaseUUIDModel):
    __tablename__ = "api_pricing_plan"
    name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(String)
    requests_limit: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="Limite quotidienne de requêtes API"
    )
    max_distinct_vins: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Nombre maximal de VINs distincts autorisés par jour",
    )
    price_per_request: Mapped[Decimal] = mapped_column(
        Numeric(10, 4), nullable=False, comment="Prix par requête en euros"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class ApiUserPricing(BaseUUIDModel):
    __tablename__ = "api_user_pricing"
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id"))
    pricing_plan_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("api_pricing_plan.id"), nullable=False
    )
    custom_requests_limit: Mapped[int | None] = mapped_column(
        Integer, comment="Limite personnalisée qui remplace celle du plan si définie"
    )
    custom_max_distinct_vins: Mapped[int | None] = mapped_column(
        Integer,
        comment="Limite de VINs distincts personnalisée qui remplace celle du plan si définie",
    )
    custom_price_per_request: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), comment="Prix personnalisé qui remplace celui du plan si défini"
    )
    effective_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Date d'entrée en vigueur de ce plan pour cet utilisateur",
    )
    expiration_date: Mapped[date | None] = mapped_column(
        Date, comment="Date d'expiration si applicable"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # Index pour les recherches rapides
    __table_args__ = (
        Index("ix_user_pricing_user_id", "user_id"),
        Index("ix_user_pricing_pricing_plan_id", "pricing_plan_id"),
    )


class ApiCallLog(BaseUUIDModel):
    __tablename__ = "api_call_log"
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"), nullable=False)
    vin: Mapped[str] = mapped_column(String(50), nullable=False)
    endpoint: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Point d'accès appelé (ex: /vehicle/static)",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    response_time: Mapped[float | None] = mapped_column(
        Float, comment="Temps de réponse en millisecondes"
    )
    status_code: Mapped[int | None] = mapped_column(
        Integer, comment="Code de statut HTTP de la réponse"
    )
    is_billed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    billed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Index pour les recherches rapides et l'efficacité des requêtes
    __table_args__ = (
        Index("ix_api_call_log_user_id", "user_id"),
        Index("ix_api_call_log_vin", "vin"),
        Index("ix_api_call_log_timestamp", "timestamp"),
        Index("ix_api_call_log_is_billed", "is_billed"),
    )


class ApiBilling(BaseUUIDModel):
    __tablename__ = "api_billing"
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    total_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    distinct_vins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, default=0
    )
    invoice_number: Mapped[str | None] = mapped_column(String(50))
    paid: Mapped[bool] = mapped_column(Boolean, default=False)
    payment_date: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # Index pour les performances
    __table_args__ = (
        Index("ix_api_billing_user_id", "user_id"),
        Index("ix_api_billing_period", "period_start", "period_end"),
        Index("ix_api_billing_paid", "paid"),
    )
