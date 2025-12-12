import json
import os
import uuid
from datetime import datetime
from typing import Any

import requests
from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from core.sql_utils import get_sqlalchemy_engine
from db_models.vehicle import Battery, Make, Oem, VehicleModel


def fetch_api_data(url: str) -> list | None:
    """Fetch data from the EV database API."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched {len(data)} vehicles from the API")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error making the request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def get_oem(session: Session, vehicle_make: str) -> uuid.UUID | None:
    """Get an OEM record and return its ID."""
    if isinstance(vehicle_make, str):
        oem = (
            session.query(Oem)
            .join(Make, Make.oem_id == Oem.id)
            .filter(func.lower(Oem.oem_name) == vehicle_make.lower())
            .first()
        )
        return oem.id if oem else None
    else:
        make = session.query(Make).filter(Make.id == vehicle_make).first()
        return make.oem_id if make else None


def get_commissioning_date(vehicle):
    "Get commissionning date and format it for the database"
    start_date = datetime.strptime(
        vehicle.get("Availability_Date_From"), "%m-%Y"
    ).date()
    try:
        end_date = datetime.strptime(
            vehicle.get("Availability_Date_To"), "%m-%Y"
        ).date()
    except:
        end_date = vehicle.get("Availability_Date_To")

    return start_date, end_date


def get_or_create_make(
    session: Session, vehicle_make: str, oem_id: uuid.UUID | None
) -> uuid.UUID:
    """Get or create a Make record and return its ID."""
    make = (
        session.query(Make)
        .filter(func.lower(Make.make_name) == vehicle_make.lower())
        .first()
    )

    if not make:
        make = Make(
            id=uuid.uuid4(),
            make_name=vehicle_make.lower(),
            oem_id=oem_id,
        )
        session.add(make)
        session.flush()
        print(f"Created new make: {vehicle_make}")
    else:
        # Update oem_id if it's different
        if make.oem_id != oem_id:
            make.oem_id = oem_id
            session.flush()

    return make.id


def standardize_battery_chemistry(battery_chemistry: str | None) -> str:
    """Standardize battery chemistry values."""
    if not battery_chemistry:
        return None
    if "nmc" in battery_chemistry.lower() or "ncm" in battery_chemistry.lower():
        return "NMC"
    return battery_chemistry


def get_or_create_battery(session: Session, vehicle: dict[str, Any]) -> uuid.UUID:
    """Get or create a Battery record and return its ID."""

    battery_chemistry = standardize_battery_chemistry(vehicle.get("Battery_Chemistry"))
    battery_manufacturer = vehicle.get("Battery_Manufacturer")
    capacity = vehicle.get("Battery_Capacity_Full")
    net_capacity = vehicle.get("Battery_Capacity_Useable")
    estimated_capacity = vehicle.get("Battery_Capacity_Estimate")
    battery_modules = vehicle.get("Battery_Modules")
    battery_cells = vehicle.get("Battery_Cells")
    battery_weight = vehicle.get("Battery_Weight")
    battery_architecture = vehicle.get("Battery_Architecture")
    battery_tms = vehicle.get("Battery_TMS")
    battery_type = vehicle.get("Battery_Type")
    battery_voltage_nominal = vehicle.get("Battery_Voltage_Nominal")
    battery_warranty_period = vehicle.get("Battery_Warranty_Period")
    battery_warranty_mileage = vehicle.get("Battery_Warranty_Mileage")

    # Helper function to build filter conditions
    def add_filter(column, value, use_upper=False):
        if value is not None:
            if use_upper:
                return func.upper(column) == value.upper()
            return column == value
        return column.is_(None)

    # Build filter conditions
    filter_conditions = [
        add_filter(Battery.battery_chemistry, battery_chemistry, use_upper=True),
        add_filter(Battery.battery_oem, battery_manufacturer, use_upper=True),
        add_filter(Battery.battery_modules, battery_modules),
        add_filter(Battery.battery_cells, battery_cells),
        add_filter(Battery.battery_weight, battery_weight),
        add_filter(Battery.battery_architecture, battery_architecture),
        add_filter(Battery.battery_tms, battery_tms),
        add_filter(Battery.battery_voltage_nominal, battery_voltage_nominal),
        add_filter(Battery.battery_warranty_period, battery_warranty_period),
        add_filter(Battery.battery_warranty_mileage, battery_warranty_mileage),
        add_filter(Battery.capacity, capacity),
        add_filter(Battery.net_capacity, net_capacity),
        add_filter(Battery.estimated_capacity, estimated_capacity),
        add_filter(Battery.battery_type, battery_type),
    ]

    battery = session.query(Battery).filter(*filter_conditions).first()

    if not battery:
        battery = Battery(
            id=uuid.uuid4(),
            battery_chemistry=battery_chemistry.upper()
            if battery_chemistry is not None
            else None,
            battery_oem=battery_manufacturer.upper()
            if battery_manufacturer is not None
            else None,
            capacity=capacity,
            net_capacity=net_capacity,
            estimated_capacity=estimated_capacity,
            battery_type=battery_type,
            battery_modules=battery_modules,
            battery_cells=battery_cells,
            battery_weight=battery_weight,
            battery_architecture=battery_architecture,
            battery_tms=battery_tms,
            battery_voltage_nominal=battery_voltage_nominal,
            battery_warranty_period=battery_warranty_period,
            battery_warranty_mileage=battery_warranty_mileage,
        )
        session.add(battery)
        session.flush()
        print(f"Created new battery: Battery with id {battery.id}")
    else:
        # Map field names to values with optional transformations
        field_updates = {
            "battery_chemistry": battery_chemistry.upper()
            if battery_chemistry is not None
            else None,
            "battery_oem": battery_manufacturer.upper()
            if battery_manufacturer is not None
            else None,
            "capacity": capacity,
            "net_capacity": net_capacity,
            "estimated_capacity": estimated_capacity,
            "battery_type": battery_type,
            "battery_modules": battery_modules,
            "battery_cells": battery_cells,
            "battery_weight": battery_weight,
            "battery_architecture": battery_architecture,
            "battery_tms": battery_tms,
            "battery_voltage_nominal": battery_voltage_nominal,
            "battery_warranty_period": battery_warranty_period,
            "battery_warranty_mileage": battery_warranty_mileage,
        }

        # Update only non-None values
        for field_name, value in field_updates.items():
            if value is not None:
                setattr(battery, field_name, value)

        session.flush()
        print(f"Updated existing battery with id {battery.id}")

    return battery.id


def get_or_create_vehicle_model(
    session: Session, vehicle: dict[str, Any], make_id: uuid.UUID, battery_id: uuid.UUID
) -> None:
    """Get or create a Vehicle Model record."""

    vehicle_id = vehicle.get("Vehicle_ID")
    evdb_model_id = str(vehicle_id) if vehicle_id is not None else None
    vehicle_model = vehicle.get("Vehicle_Model")
    type_car = vehicle.get("Vehicle_Model_Version")
    version = None

    # Early validation to prevent AttributeError on None values
    if vehicle_model is None:
        print(f"Warning: Vehicle_Model is None for vehicle ID {evdb_model_id}")

    if (
        vehicle_model
        and isinstance(vehicle_model, str)
        and vehicle_model.lower() == "zoe"
        and type_car
        and isinstance(type_car, str)
    ):
        # Define the patterns to look for in the type string
        type_patterns = ["q210", "r240", "r90", "r110", "q90", "r135"]

        # Check if any of the patterns exist in the type string
        found_type = None
        for pattern in type_patterns:
            if pattern.lower() in type_car.lower():
                found_type = pattern.lower()
                break

        if found_type:
            # Extract the found pattern as type and the rest as version
            type_parts = type_car.strip().split()
            # Remove the found pattern from type_parts and join the rest as version
            remaining_parts = [
                part for part in type_parts if found_type not in part.lower()
            ]
            type_car = found_type
            version = " ".join(remaining_parts) if remaining_parts else None
        else:
            # Fallback to original logic if no pattern is found
            type_parts = type_car.strip().split()
            type_car = type_parts[0] if type_parts else None
            version = " ".join(type_parts[1:]) if len(type_parts) > 1 else None

    # Query by evdb_model_id
    model = (
        session.query(VehicleModel)
        .filter(VehicleModel.evdb_model_id == evdb_model_id)
        .first()
    )

    start_date, end_date = get_commissioning_date(vehicle)
    oem_id = get_oem(session, vehicle.get("Vehicle_Make", ""))

    if not model:
        model = VehicleModel(
            id=uuid.uuid4(),
            model_name=vehicle_model.lower() if vehicle_model else "unknown",
            type=type_car.lower() if type_car else None,
            version=version.lower() if version else None,
            make_id=make_id,
            oem_id=oem_id,
            autonomy=vehicle.get("Range_WLTP"),
            expected_consumption=vehicle.get("Efficiency_Consumption_Real"),
            warranty_date=vehicle.get("Battery_Warranty_Period"),
            warranty_km=vehicle.get("Battery_Warranty_Mileage"),
            source=vehicle.get("EVDB_Detail_URL"),
            battery_id=battery_id,
            commissioning_date=start_date,
            end_of_life_date=end_date,
            evdb_model_id=evdb_model_id,
            charge_plug_location=vehicle.get("Charge_Plug_Location"),
            charge_plug_type=vehicle.get("Charge_Plug_Type"),
            fast_charge_max_power=vehicle.get("Fastcharge_Power_Max"),
            fast_charge_duration=vehicle.get("Fastcharge_ChargeTime"),
            standard_charge_duration=vehicle.get("Charge_Standard_ChargeTime"),
            ac_charge_duration=vehicle.get("Charge_Alternative_ChargeTime"),
            autonomy_city_winter=vehicle.get("Range_Real_WCty"),
            autonomy_city_summer=vehicle.get("Range_Real_BCty"),
            autonomy_highway_winter=vehicle.get("Range_Real_WHwy"),
            autonomy_highway_summer=vehicle.get("Range_Real_BHwy"),
            autonomy_combined_winter=vehicle.get("Range_Real_WCmb"),
            autonomy_combined_summer=vehicle.get("Range_Real_BCmb"),
            maximum_speed=vehicle.get("Performance_Topspeed"),
        )
        session.add(model)
        session.flush()
        print(
            f"Created new vehicle model with id {evdb_model_id}: {vehicle_model} {type_car} {version}"
        )
    else:
        # Update existing model with COALESCE-like behavior (only update if new value is not None)
        if vehicle.get("Range_WLTP") is not None:
            model.autonomy = vehicle.get("Range_WLTP")
        if vehicle.get("Efficiency_Consumption_Real") is not None:
            model.expected_consumption = vehicle.get("Efficiency_Consumption_Real")
        if vehicle.get("Battery_Warranty_Period") is not None:
            model.warranty_date = vehicle.get("Battery_Warranty_Period")
        if vehicle.get("Battery_Warranty_Mileage") is not None:
            model.warranty_km = vehicle.get("Battery_Warranty_Mileage")
        if vehicle.get("EVDB_Detail_URL") is not None:
            model.source = vehicle.get("EVDB_Detail_URL")
        model.commissioning_date = start_date
        model.end_of_life_date = end_date
        if battery_id is not None:
            model.battery_id = battery_id
        if evdb_model_id is not None:
            model.evdb_model_id = evdb_model_id
        if vehicle.get("Charge_Plug_Location") is not None:
            model.charge_plug_location = vehicle.get("Charge_Plug_Location")
        if vehicle.get("Charge_Plug_Type") is not None:
            model.charge_plug_type = vehicle.get("Charge_Plug_Type")
        if vehicle.get("Fastcharge_Power_Max") is not None:
            model.fast_charge_max_power = vehicle.get("Fastcharge_Power_Max")
        if vehicle.get("Fastcharge_ChargeTime") is not None:
            model.fast_charge_duration = vehicle.get("Fastcharge_ChargeTime")
        if vehicle.get("Charge_Standard_ChargeTime") is not None:
            model.standard_charge_duration = vehicle.get("Charge_Standard_ChargeTime")
        if vehicle.get("Charge_Alternative_ChargeTime") is not None:
            model.ac_charge_duration = vehicle.get("Charge_Alternative_ChargeTime")
        if vehicle.get("Range_Real_WCty") is not None:
            model.autonomy_city_winter = vehicle.get("Range_Real_WCty")
        if vehicle.get("Range_Real_BCty") is not None:
            model.autonomy_city_summer = vehicle.get("Range_Real_BCty")
        if vehicle.get("Range_Real_WHwy") is not None:
            model.autonomy_highway_winter = vehicle.get("Range_Real_WHwy")
        if vehicle.get("Range_Real_BHwy") is not None:
            model.autonomy_highway_summer = vehicle.get("Range_Real_BHwy")
        if vehicle.get("Range_Real_WCmb") is not None:
            model.autonomy_combined_winter = vehicle.get("Range_Real_WCmb")
        if vehicle.get("Range_Real_BCmb") is not None:
            model.autonomy_combined_summer = vehicle.get("Range_Real_BCmb")
        if vehicle.get("Performance_Topspeed") is not None:
            model.maximum_speed = vehicle.get("Performance_Topspeed")
        session.flush()
        print(f"Updated existing vehicle model: {vehicle_model} {type_car} {version}")


def process_vehicle(session: Session, vehicle: dict[str, Any]) -> None:
    """Process a single vehicle record."""
    try:
        if vehicle.get("Vehicle_Make", "").lower() == "tesla":
            print(
                f"Skipping Tesla model: {vehicle.get('Vehicle_Model')} {vehicle.get('Vehicle_Model_Version')}"
            )
            return

        oem_id = get_oem(session, vehicle.get("Vehicle_Make", ""))
        make_id = get_or_create_make(session, vehicle.get("Vehicle_Make", ""), oem_id)
        battery_id = get_or_create_battery(session, vehicle)
        print(battery_id)
        get_or_create_vehicle_model(session, vehicle, make_id, battery_id)
    except Exception as e:
        print(f"Error processing vehicle {vehicle.get('Vehicle_Model')}: {e!s}")
        raise


def fetch_ev_data():
    """Main function to fetch and process EV data."""
    url = os.getenv("EV_DATABASE_URL")
    print(f"Fetching data from URL: {url}")

    data = fetch_api_data(url)
    if not data:
        return None

    engine = get_sqlalchemy_engine()
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        try:
            for vehicle in data:
                process_vehicle(session, vehicle)
            session.commit()
            print("Successfully processed all vehicle models")
        except Exception as e:
            session.rollback()
            print(f"Error processing vehicles: {e!s}")
            raise

    return data


if __name__ == "__main__":
    fetch_ev_data()
