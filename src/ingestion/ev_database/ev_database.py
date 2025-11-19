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
        return "unknown"
    if "nmc" in battery_chemistry.lower() or "ncm" in battery_chemistry.lower():
        return "NMC"
    return battery_chemistry


def get_or_create_battery(session: Session, vehicle: dict[str, Any]) -> uuid.UUID:
    """Get or create a Battery record and return its ID."""
    battery_chemistry = standardize_battery_chemistry(vehicle.get("Battery_Chemistry"))
    battery_manufacturer = vehicle.get("Battery_Manufacturer") or "unknown"
    battery_name = vehicle.get("Vehicle_Model", "unknown")
    capacity = vehicle.get("Battery_Capacity_Full")
    net_capacity = vehicle.get("Battery_Capacity_Useable")

    battery = (
        session.query(Battery)
        .filter(
            func.upper(Battery.battery_name) == battery_name.upper(),
            func.upper(Battery.battery_chemistry) == battery_chemistry.upper(),
            func.upper(Battery.battery_oem) == battery_manufacturer.upper(),
            Battery.capacity == capacity,
            Battery.net_capacity == net_capacity,
        )
        .first()
    )

    if not battery:
        battery = Battery(
            id=uuid.uuid4(),
            battery_name=battery_name,
            battery_chemistry=battery_chemistry.upper(),
            battery_oem=battery_manufacturer.upper(),
            capacity=capacity,
            net_capacity=net_capacity,
            source=vehicle.get("EVDB_Detail_URL"),
        )
        session.add(battery)
        session.flush()
        print(
            f"Created new battery: Battery Name={battery_name}, Chemistry={battery_chemistry}, "
            f"OEM={battery_manufacturer}, Capacity={capacity}kWh, Net Capacity={net_capacity}kWh"
        )
    else:
        # Update battery fields
        battery.battery_name = battery_name
        battery.battery_chemistry = battery_chemistry.upper()
        battery.battery_oem = battery_manufacturer.upper()
        battery.capacity = capacity
        battery.net_capacity = net_capacity
        battery.source = vehicle.get("EVDB_Detail_URL")
        session.flush()
        print(
            f"Updated existing battery: Battery Name={battery_name}, Chemistry={battery_chemistry}, "
            f"OEM={battery_manufacturer}, Capacity={capacity}kWh, Net Capacity={net_capacity}kWh"
        )

    return battery.id


def get_or_create_vehicle_model(
    session: Session, vehicle: dict[str, Any], make_id: uuid.UUID, battery_id: uuid.UUID
) -> None:
    """Get or create a Vehicle Model record."""
    vehicle_id = vehicle.get("Vehicle_ID")
    evdb_model_id = str(vehicle_id) if vehicle_id is not None else None
    vehicle_model = vehicle.get("Vehicle_Model", "unknown")
    type_car = vehicle.get("Vehicle_Model_Version") or "unknown"
    version = "unknown"

    if vehicle_model.lower() == "zoe":
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
            version = " ".join(remaining_parts) if remaining_parts else "unknown"
        else:
            # Fallback to original logic if no pattern is found
            type_parts = type_car.strip().split()
            type_car = type_parts[0] if type_parts else "unknown"
            version = " ".join(type_parts[1:]) if len(type_parts) > 1 else "unknown"

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
            model_name=vehicle_model.lower(),
            type=type_car.lower(),
            version=version.lower(),
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
        if start_date is not None:
            model.commissioning_date = start_date
        if end_date is not None:
            model.end_of_life_date = end_date
        if battery_id is not None:
            model.battery_id = battery_id
        if evdb_model_id is not None:
            model.evdb_model_id = evdb_model_id

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
