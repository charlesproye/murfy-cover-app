"""Models for vehicle-related tables"""

import uuid
from datetime import UTC, datetime

from db_models.enums import LanguageEnum
from sqlalchemy import (
    JSON,
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
)
from sqlalchemy.orm import Mapped
from sqlalchemy import Enum as SqlEnum

from db_models.base_uuid_model import BaseUUIDModel


class Company(BaseUUIDModel):
    __tablename__ = "company"
    name: str = Column(String(100), nullable=False)
    description: str = Column(String, nullable=True)


class Role(BaseUUIDModel):
    __tablename__ = "role"
    role_name: str = Column(String(50), nullable=False)


class User(BaseUUIDModel):
    __tablename__ = "user"
    company_id: Mapped[uuid.UUID] = Column(ForeignKey("company.id"), nullable=False)
    first_name: str = Column(String(100), nullable=False)
    last_name: str = Column(String(100))
    last_connection = Column(DateTime)
    email: str = Column(String(100), nullable=False, unique=True)
    password: str = Column(String(100))
    phone: str = Column(String(20))
    role_id: Mapped[uuid.UUID] = Column(ForeignKey("role.id"))
    is_active: bool = Column(Boolean, default=True)


class Fleet(BaseUUIDModel):
    __tablename__ = "fleet"
    fleet_name: str = Column(String(100), nullable=False)
    company_id: Mapped[uuid.UUID] = Column(ForeignKey("company.id"), nullable=False)


class FleetAggregate(BaseUUIDModel):
    __tablename__ = "fleet_aggregate"
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    avg_soh = Column(Numeric(5, 2))
    avg_value = Column(Numeric(5, 2))
    avg_energy_consumption = Column(Numeric(10, 2))
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))


class UserFleet(BaseUUIDModel):
    __tablename__ = "user_fleet"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    role_id: Mapped[uuid.UUID] = Column(ForeignKey("role.id"), nullable=False)


class Region(BaseUUIDModel):
    __tablename__ = "region"
    region_name: str = Column(String(100), nullable=False)


class VehicleModel(BaseUUIDModel):
    __tablename__ = "vehicle_model"
    model_name: str = Column(String(100), nullable=False)
    type: str = Column(String(50))
    version: str = Column(String(50))
    oem_id: Mapped[uuid.UUID] = Column(ForeignKey("oem.id"))
    make_id: Mapped[uuid.UUID] = Column(ForeignKey("make.id"))
    battery_id: Mapped[uuid.UUID] = Column(ForeignKey("battery.id"))
    autonomy = Column(Integer)
    url_image: str = Column(String(2000))
    warranty_date = Column(Integer)
    warranty_km = Column(Numeric(10, 2))
    source: str = Column(String(100))
    trendline = Column(JSON)
    trendline_min = Column(JSON)
    trendline_max = Column(JSON)
    trendline_bib: bool = Column(
        Boolean,
        default=False,
        comment="If the trendline is based on SoH calculated by BIB",
    )
    odometer_data: bool = Column(Boolean, default=False)
    soh_data: bool = Column(Boolean, default=False)
    soh_oem_data: bool = Column(Boolean, default=False)
    commissioning_date = Column(DateTime, comment="First time seen on the market")
    end_of_life_date = Column(DateTime, comment="Last time seen on the market")
    expected_consumption = Column(Integer)


class Battery(BaseUUIDModel):
    __tablename__ = "battery"
    battery_name: str = Column(String(100))
    source: str = Column(String(100))
    battery_chemistry: str = Column(String(100))
    battery_oem: str = Column(String(100))
    capacity = Column(Numeric(10, 2))
    net_capacity = Column(Numeric(10, 2))


class Vehicle(BaseUUIDModel):
    __tablename__ = "vehicle"
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    region_id: Mapped[uuid.UUID] = Column(ForeignKey("region.id"), nullable=False)
    vehicle_model_id: Mapped[uuid.UUID] = Column(
        ForeignKey("vehicle_model.id"), nullable=False
    )
    vin: str = Column(String(50))
    activation_status: Mapped[bool] = Column(Boolean, nullable=True)
    is_eligible: Mapped[bool] = Column(Boolean, nullable=True)
    is_pinned: Mapped[bool] = Column(Boolean, nullable=True)

    start_date = Column(Date)
    licence_plate: str = Column(String(50))
    end_of_contract_date = Column(Date)
    last_date_data = Column(Date)
    __table_args__ = (
        Index("ix_vehicle_fleet_id", "fleet_id"),  # Index sur l'ID de la flotte
        Index("ix_vehicle_region_id", "region_id"),  # Index sur l'ID de la région
        Index(
            "ix_vehicle_model_id", "vehicle_model_id"
        ),  # Index sur l'ID du modèle de véhicule
        Index("ix_vehicle_vin", "vin"),  # Index sur le VIN
    )


class VehicleAggregate(BaseUUIDModel):
    __tablename__ = "vehicle_aggregate"
    vehicle_model_id: Mapped[uuid.UUID] = Column(
        ForeignKey("vehicle_model.id"), nullable=False
    )
    avg_soh = Column(Numeric(5, 2))
    energy_consumption = Column(Numeric(10, 2))
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))


class VehicleData(BaseUUIDModel):
    __tablename__ = "vehicle_data"
    vehicle_id: Mapped[uuid.UUID] = Column(ForeignKey("vehicle.id"), nullable=False)
    odometer = Column(Numeric(10, 2))
    region: str = Column(String(100))
    speed = Column(Numeric(5, 2))
    location = Column(String(100))
    soh = Column(Numeric(5, 3))
    cycles = Column(Numeric(10, 2))
    consumption = Column(Numeric(5, 3))
    soh_comparison = Column(Numeric(6, 3))
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))
    level_1 = Column(
        Numeric(6, 2),
        comment="Level 1 of charging. Corresponds to charging in the range 1.4-1.9 kW, 120V, AC, 12-16 Ah",
    )
    level_2 = Column(
        Numeric(6, 2),
        comment="Level 2 of charging. Corresponds to charging in the range 1.9.3-19.2 kW, 208V, AC, 32-64 Ah",
    )
    level_3 = Column(
        Numeric(6, 2),
        comment="Level 3 of charging. Corresponds to charging in the range > 50kW, DC",
    )
    soh_oem = Column(Numeric(5, 2))


class Oem(BaseUUIDModel):
    __tablename__ = "oem"
    oem_name: str = Column(String(100), nullable=False)
    description: str = Column(String)
    trendline = Column(JSON)
    trendline_min = Column(JSON)
    trendline_max = Column(JSON)


class Make(BaseUUIDModel):
    __tablename__ = "make"
    make_name: str = Column(String(100), nullable=False)
    oem_id: Mapped[uuid.UUID] = Column(ForeignKey("oem.id"))
    description: str = Column(String)


class RegionalAggregate(BaseUUIDModel):
    __tablename__ = "regional_aggregate"
    region_id: Mapped[uuid.UUID] = Column(ForeignKey("region.id"), nullable=False)
    avg_soh = Column(Numeric(5, 2))
    avg_soc = Column(Numeric(5, 2))
    avg_temperature = Column(Numeric(5, 2))
    avg_voltage = Column(Numeric(10, 2))
    energy_consumption = Column(Numeric(10, 2))
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC))


class ApiUser(BaseUUIDModel):
    __tablename__ = "api_user"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    api_key: str = Column(String(100), unique=True, nullable=False)
    is_active: bool = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
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
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


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
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

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
    timestamp = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
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
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(
        DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # Index pour les performances
    __table_args__ = (
        Index("ix_api_billing_user_id", "user_id"),
        Index("ix_api_billing_period", "period_start", "period_end"),
        Index("ix_api_billing_paid", "paid"),
    )


class FlashReportCombination(BaseUUIDModel):
    __tablename__ = "flash_report_combination"
    vin: str = Column(String, nullable=False)
    make: str = Column(String, nullable=False)
    model: str = Column(String, nullable=False)
    type: str = Column(String, nullable=False)
    version: str | None = Column(String, nullable=True)
    odometer: int = Column(Integer, nullable=False)
    token: str = Column(String, nullable=False, unique=True)

    language: LanguageEnum = Column(
        SqlEnum(LanguageEnum, name="language_enum"), 
        nullable=False, 
        default=LanguageEnum.EN
    )
