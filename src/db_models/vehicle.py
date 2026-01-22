"""Models for vehicle-related tables"""

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import (
    BaseAutoIncrementModel,
    BaseUUIDCreatedAt,
    BaseUUIDModel,
)


class Region(BaseUUIDCreatedAt):
    __tablename__ = "region"
    region_name: Mapped[str] = mapped_column(String(100), nullable=False)


class VehicleModel(BaseUUIDModel):
    __tablename__ = "vehicle_model"
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)
    type: Mapped[str | None] = mapped_column(String(50))
    version: Mapped[str | None] = mapped_column(String(50))
    oem_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("oem.id"))
    make_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("make.id"))
    battery_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("battery.id"))
    autonomy: Mapped[int | None] = mapped_column(Integer)
    image_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("asset.id"))
    warranty_date: Mapped[int | None] = mapped_column(Integer)
    warranty_km: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    source: Mapped[str | None] = mapped_column(String(100))
    trendline_bib: Mapped[str | None] = mapped_column(String(2000))
    trendline_bib_min: Mapped[str | None] = mapped_column(String(2000))
    trendline_bib_max: Mapped[str | None] = mapped_column(String(2000))
    trendline_oem: Mapped[str | None] = mapped_column(String(2000))
    trendline_oem_min: Mapped[str | None] = mapped_column(String(2000))
    trendline_oem_max: Mapped[str | None] = mapped_column(String(2000))
    has_trendline_bib: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        comment="If the trendline is based on SoH calculated by BIB",
    )
    has_trendline_oem: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        comment="If the trendline is based on SoH calculated by Readout",
    )
    odometer_data: Mapped[bool] = mapped_column(Boolean, default=False)
    soh_data: Mapped[bool] = mapped_column(Boolean, default=False)
    soh_oem_data: Mapped[bool] = mapped_column(Boolean, default=False)
    commissioning_date: Mapped[datetime | None] = mapped_column(
        DateTime, comment="First time seen on the market"
    )
    end_of_life_date: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Last time seen on the market"
    )
    expected_consumption: Mapped[int | None] = mapped_column(Integer)
    evdb_model_id: Mapped[str | None] = mapped_column(String(100))
    maximum_speed: Mapped[int | None] = mapped_column(Integer)
    charge_plug_location: Mapped[str | None] = mapped_column(String(100))
    charge_plug_type: Mapped[str | None] = mapped_column(String(100))
    fast_charge_plug_type: Mapped[str | None] = mapped_column(String(100))
    fast_charge_max_power: Mapped[int | None] = mapped_column(Integer)
    fast_charge_duration: Mapped[int | None] = mapped_column(Integer)
    standard_charge_duration: Mapped[int | None] = mapped_column(Integer)
    ac_charge_duration: Mapped[int | None] = mapped_column(Integer)
    autonomy_city_winter: Mapped[int | None] = mapped_column(Integer)
    autonomy_city_summer: Mapped[int | None] = mapped_column(Integer)
    autonomy_highway_winter: Mapped[int | None] = mapped_column(Integer)
    autonomy_highway_summer: Mapped[int | None] = mapped_column(Integer)
    autonomy_combined_winter: Mapped[int | None] = mapped_column(Integer)
    autonomy_combined_summer: Mapped[int | None] = mapped_column(Integer)
    real_autonomy: Mapped[int | None] = mapped_column(
        Integer, comment="Autonomy calculated by Bib"
    )


class Battery(BaseUUIDModel):
    __tablename__ = "battery"
    battery_type: Mapped[str | None] = mapped_column(String(100))
    battery_chemistry: Mapped[str | None] = mapped_column(String(100))
    battery_oem: Mapped[str | None] = mapped_column(String(100))
    capacity: Mapped[float | None] = mapped_column(Numeric(10, 2))
    net_capacity: Mapped[float | None] = mapped_column(Numeric(10, 2))
    estimated_capacity: Mapped[str | None] = mapped_column(String(100))
    battery_modules: Mapped[int | None] = mapped_column(Integer)
    battery_cells: Mapped[str | None] = mapped_column(String(255))
    battery_weight: Mapped[float | None] = mapped_column(Numeric(10, 2))
    battery_architecture: Mapped[str | None] = mapped_column(String(100))
    battery_tms: Mapped[str | None] = mapped_column(String(100))
    battery_voltage_nominal: Mapped[float | None] = mapped_column(Numeric(10, 2))
    battery_warranty_period: Mapped[int | None] = mapped_column(Integer)
    battery_warranty_mileage: Mapped[float | None] = mapped_column(Numeric(10, 2))


class Vehicle(BaseUUIDModel):
    __tablename__ = "vehicle"
    fleet_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("fleet.id"), nullable=False)
    region_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("region.id"), nullable=False
    )
    vehicle_model_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("vehicle_model.id"), nullable=True
    )
    vin: Mapped[str | None] = mapped_column(String(50))
    bib_score: Mapped[str | None] = mapped_column(String(1))
    activation_status: Mapped[bool | None] = mapped_column(Boolean)
    bib_report_requested_status: Mapped[bool | None] = mapped_column(Boolean)
    readout_report_requested_status: Mapped[bool | None] = mapped_column(Boolean)
    activation_requested_status: Mapped[bool | None] = mapped_column(Boolean)
    activation_start_date: Mapped[date | None] = mapped_column(Date)
    activation_end_date: Mapped[date | None] = mapped_column(Date)
    activation_status_message: Mapped[str | None] = mapped_column(String(255))
    activation_comment: Mapped[str | None] = mapped_column(String(255))
    is_eligible: Mapped[bool | None] = mapped_column(Boolean)
    is_pinned: Mapped[bool | None] = mapped_column(Boolean)
    start_date: Mapped[date | None] = mapped_column(Date)
    licence_plate: Mapped[str | None] = mapped_column(String(50))
    end_of_contract_date: Mapped[date | None] = mapped_column(Date)
    is_processed: Mapped[bool] = mapped_column(Boolean, nullable=True)
    __table_args__ = (
        Index("ix_vehicle_fleet_id", "fleet_id"),  # Index sur l'ID de la flotte
        Index("ix_vehicle_region_id", "region_id"),  # Index sur l'ID de la région
        Index(
            "ix_vehicle_model_id", "vehicle_model_id"
        ),  # Index sur l'ID du modèle de véhicule
        Index("ix_vehicle_vin", "vin"),  # Index sur le VIN
    )


class VehicleData(BaseUUIDModel):
    __tablename__ = "vehicle_data"
    vehicle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("vehicle.id"), nullable=False
    )
    odometer: Mapped[float | None] = mapped_column(Numeric(10, 2))
    region: Mapped[str | None] = mapped_column(String(100))
    speed: Mapped[float | None] = mapped_column(Numeric(5, 2))
    location: Mapped[str | None] = mapped_column(String(100))
    soh_bib: Mapped[float | None] = mapped_column(Numeric(5, 3))
    cycles: Mapped[float | None] = mapped_column(Numeric(10, 2))
    consumption: Mapped[float | None] = mapped_column(Numeric(5, 3))
    soh_comparison: Mapped[float | None] = mapped_column(Numeric(6, 3))
    timestamp: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    level_1: Mapped[float | None] = mapped_column(
        Numeric(6, 2),
        comment="Level 1 of charging. Corresponds to charging in the range 1.4-1.9 kW, 120V, AC, 12-16 Ah",
    )
    level_2: Mapped[float | None] = mapped_column(
        Numeric(6, 2),
        comment="Level 2 of charging. Corresponds to charging in the range 1.9.3-19.2 kW, 208V, AC, 32-64 Ah",
    )
    level_3: Mapped[float | None] = mapped_column(
        Numeric(6, 2),
        comment="Level 3 of charging. Corresponds to charging in the range > 50kW, DC",
    )
    soh_oem: Mapped[float | None] = mapped_column(Numeric(5, 2))
    real_autonomy: Mapped[float | None] = mapped_column(Numeric(10, 0))
    timestamp_last_data_collected: Mapped[datetime | None] = mapped_column(DateTime)


class VehicleStatus(BaseUUIDModel):
    __tablename__ = "vehicle_status"
    vehicle_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("vehicle.id"))
    vin: Mapped[str] = mapped_column(String(50), nullable=False)
    status_name: Mapped[str] = mapped_column(String(100), nullable=False)
    status_value: Mapped[bool] = mapped_column(Boolean, nullable=False)
    process_step: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class FleetTeslaAuthenticationCode(BaseUUIDModel):
    __tablename__ = "fleet_tesla_authentication_code"
    fleet_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("fleet.id"), nullable=False, unique=True
    )
    authentication_code: Mapped[str] = mapped_column(String(2000), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class VehicleActivationHistory(BaseAutoIncrementModel):
    __tablename__ = "vehicle_activation_history"
    vehicle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("vehicle.id"), nullable=False
    )
    activation_requested_status: Mapped[bool] = mapped_column(Boolean, nullable=False)
    activation_status: Mapped[bool] = mapped_column(Boolean, nullable=False)
    activation_status_message: Mapped[str] = mapped_column(String(255), nullable=False)
    oem_detail: Mapped[str] = mapped_column(String(555), nullable=True)
