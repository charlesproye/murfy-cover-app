"""Factories for vehicle-related models."""

from uuid import uuid4

from polyfactory import Use

from db_models.vehicle import (
    Battery,
    Region,
    Vehicle,
    VehicleData,
    VehicleModel,
    VehicleStatus,
)
from tests.factories.base import BaseAsyncFactory


class RegionFactory(BaseAsyncFactory[Region]):
    """Factory for Region model."""

    __model__ = Region

    region_name = "France"


class BatteryFactory(BaseAsyncFactory[Battery]):
    """Factory for Battery model."""

    __model__ = Battery

    battery_type = "LFP"
    battery_chemistry = "LFP"
    battery_oem = "CATL"
    capacity = 75.0
    net_capacity = 72.0


class VehicleModelFactory(BaseAsyncFactory[VehicleModel]):
    """Factory for VehicleModel model."""

    __model__ = VehicleModel

    # oem_id, make_id, battery_id must be provided
    model_name = "model 3"
    type = "long range awd"
    version = "2021"
    autonomy = 580
    source = "test"
    url_image = None
    warranty_date = None
    warranty_km = None

    # Default trendline: y = 100 - 0.0001 * x (simulates degradation)
    trendline = Use(lambda: {"trendline": "100 - 0.0001 * x"})
    trendline_min = Use(lambda: {"trendline": "95 - 0.0001 * x"})
    trendline_max = Use(lambda: {"trendline": "105 - 0.0001 * x"})


class VehicleFactory(BaseAsyncFactory[Vehicle]):
    """Factory for Vehicle model."""

    __model__ = Vehicle

    # fleet_id, region_id, vehicle_model_id must be provided
    vin = Use(lambda: f"5YJ3E1EA1KF{uuid4().hex[:6].upper()}")
    bib_score = None
    activation_status = True
    is_eligible = True
    is_pinned = None
    start_date = None
    licence_plate = Use(lambda: f"AB-{uuid4().hex[:3].upper()}-CD")
    end_of_contract_date = None


class VehicleDataFactory(BaseAsyncFactory[VehicleData]):
    """Factory for VehicleData model."""

    __model__ = VehicleData

    # vehicle_id must be provided
    odometer = 50000.0
    region = "France"
    speed = None
    location = None
    soh = 95.5
    cycles = None
    consumption = None
    soh_comparison = None
    level_1 = None
    level_2 = None
    level_3 = None
    soh_oem = None
    real_autonomy = None
    timestamp_last_data_collected = None


class VehicleStatusFactory(BaseAsyncFactory[VehicleStatus]):
    """Factory for VehicleStatus model."""

    __model__ = VehicleStatus

    # vehicle_id can be None
    vin = Use(lambda: f"5YJ3E1EA1KF{uuid4().hex[:6].upper()}")
    status_name = "activation_status"
    status_value = True
    process_step = "ingestion"


__all__ = [
    "BatteryFactory",
    "RegionFactory",
    "VehicleDataFactory",
    "VehicleFactory",
    "VehicleModelFactory",
    "VehicleStatusFactory",
]
