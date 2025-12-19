"""Models for vehicle-related tables"""

import uuid

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDModel


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
    evdb_model_id: str | None = Column(String(100), nullable=True)
    maximum_speed: int | None = Column(Integer)
    charge_plug_location: str | None = Column(String(100))
    charge_plug_type: str | None = Column(String(100))
    fast_charge_max_power: int | None = Column(Integer)
    fast_charge_duration: int | None = Column(Integer)
    standard_charge_duration: int | None = Column(Integer)
    ac_charge_duration: int | None = Column(Integer)
    autonomy_city_winter: int | None = Column(Integer)
    autonomy_city_summer: int | None = Column(Integer)
    autonomy_highway_winter: int | None = Column(Integer)
    autonomy_highway_summer: int | None = Column(Integer)
    autonomy_combined_winter: int | None = Column(Integer)
    autonomy_combined_summer: int | None = Column(Integer)


class Battery(BaseUUIDModel):
    __tablename__ = "battery"
    battery_type = Column(String(100))
    battery_chemistry: str = Column(String(100))
    battery_oem: str = Column(String(100))
    capacity: float | None = Column(Numeric(10, 2))
    net_capacity: float | None = Column(Numeric(10, 2))
    estimated_capacity: str | None = Column(String(100))
    battery_modules: int | None = Column(Integer)
    battery_cells: str | None = Column(String(255))
    battery_weight: float | None = Column(Numeric(10, 2))
    battery_architecture: str | None = Column(String(100))
    battery_tms: str | None = Column(String(100))
    battery_voltage_nominal: float | None = Column(Numeric(10, 2))
    battery_warranty_period: int | None = Column(Integer)
    battery_warranty_mileage: float | None = Column(Numeric(10, 2))


class Vehicle(BaseUUIDModel):
    __tablename__ = "vehicle"
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    region_id: Mapped[uuid.UUID] = Column(ForeignKey("region.id"), nullable=False)
    vehicle_model_id: Mapped[uuid.UUID] = Column(
        ForeignKey("vehicle_model.id"), nullable=False
    )
    vin: str = Column(String(50))
    bib_score: str = Column(String(1), nullable=True)
    activation_status: Mapped[bool] = Column(Boolean, nullable=True)
    is_eligible: Mapped[bool] = Column(Boolean, nullable=True)
    is_pinned: Mapped[bool] = Column(Boolean, nullable=True)
    start_date = Column(Date)
    licence_plate: str = Column(String(50))
    end_of_contract_date = Column(Date)
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
    timestamp = Column(DateTime, server_default=func.now())


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
    timestamp = Column(DateTime, server_default=func.now())
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
    real_autonomy = Column(Numeric(10, 0))
    timestamp_last_data_collected = Column(DateTime)


class RegionalAggregate(BaseUUIDModel):
    __tablename__ = "regional_aggregate"
    region_id: Mapped[uuid.UUID] = Column(ForeignKey("region.id"), nullable=False)
    avg_soh = Column(Numeric(5, 2))
    avg_soc = Column(Numeric(5, 2))
    avg_temperature = Column(Numeric(5, 2))
    avg_voltage = Column(Numeric(10, 2))
    energy_consumption = Column(Numeric(10, 2))
    timestamp = Column(DateTime, server_default=func.now())


class VehicleStatus(BaseUUIDModel):
    __tablename__ = "vehicle_status"
    vehicle_id: Mapped[uuid.UUID] | None = Column(
        ForeignKey("vehicle.id"), nullable=True
    )
    vin: str = Column(String(50), nullable=False)
    status_name: str = Column(String(100), nullable=False)
    status_value: bool = Column(Boolean, nullable=False)
    process_step: str = Column(String(100), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class FleetTeslaAuthenticationCode(BaseUUIDModel):
    __tablename__ = "fleet_tesla_authentication_code"
    fleet_id: Mapped[uuid.UUID] = Column(
        ForeignKey("fleet.id"), nullable=False, unique=True
    )
    authentication_code: Mapped[str] = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
