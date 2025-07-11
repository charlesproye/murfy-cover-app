import requests
import json
import uuid
from core.sql_utils import get_connection
import os
from typing import Optional, Dict, Any
import re

def fetch_api_data(url: str) -> Optional[list]:
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

def get_oem(cursor, vehicle_make: str) -> str:
    """Get an OEM record and return its ID."""
    cursor.execute(
        "SELECT oem_id FROM make WHERE LOWER(oem_name) = LOWER(%s)",
        (vehicle_make,))
    oem_result = cursor.fetchone()
    
    return oem_result

def get_or_create_make(cursor, vehicle_make: str, oem_id: str) -> str:
    """Get or create a Make record and return its ID."""
    cursor.execute(
        "SELECT id FROM make WHERE LOWER(make_name) = LOWER(%s)",
        (vehicle_make,)
    )
    make_result = cursor.fetchone()
    
    if not make_result:
        make_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO make (id, make_name) VALUES (%s, LOWER(%s)) RETURNING id",
            (make_id, vehicle_make)
        )
        make_id = cursor.fetchone()[0]
    else:
        make_id = make_result[0]
    
    return make_id

def standardize_battery_chemistry(battery_chemistry: str) -> str:
    """Standardize battery chemistry values."""
    if not battery_chemistry:
        return "unknown"
    if 'nmc' in battery_chemistry.lower() or 'ncm' in battery_chemistry.lower():
        return 'NMC'
    return battery_chemistry

def get_or_create_battery(cursor, vehicle: Dict[str, Any]) -> str:
    """Get or create a Battery record and return its ID."""
    battery_chemistry = standardize_battery_chemistry(vehicle.get("Battery_Chemistry"))
    battery_manufacturer = vehicle.get("Battery_Manufacturer") or "unknown"
    
    cursor.execute(
        "SELECT id FROM battery WHERE UPPER(battery_name) = UPPER(%s) AND UPPER(battery_chemistry) = UPPER(%s) AND UPPER(battery_oem) = UPPER(%s) AND capacity = %s AND net_capacity = %s",
        (vehicle.get("Vehicle_Model","unknown"), battery_chemistry.upper(), 
         battery_manufacturer.upper(), 
         vehicle.get("Battery_Capacity_Full"), 
         vehicle.get("Battery_Capacity_Useable"))
    )
    battery_type_result = cursor.fetchone()
    
    if not battery_type_result:
        battery_id = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO battery (id, battery_name, battery_chemistry, battery_oem, capacity, net_capacity, source) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (battery_id, vehicle.get("Vehicle_Model","unknown"), battery_chemistry.upper(), 
             battery_manufacturer.upper(), vehicle.get("Battery_Capacity_Full"), 
             vehicle.get("Battery_Capacity_Useable"), vehicle.get("EVDB_Detail_URL"))
        )
        battery_id = cursor.fetchone()[0]
        print(f"Created new battery: Battery Name={vehicle.get('Vehicle_Model')}, Chemistry={battery_chemistry}, OEM={battery_manufacturer}, Capacity={vehicle.get('Battery_Capacity_Full')}kWh, Net Capacity={vehicle.get('Battery_Capacity_Useable')}kWh")
    else:
        battery_id = battery_type_result[0]
        print(f"Using existing battery: Battery Name={vehicle.get('Vehicle_Model')}, Chemistry={battery_chemistry}, OEM={battery_manufacturer}, Capacity={vehicle.get('Battery_Capacity_Full')}kWh, Net Capacity={vehicle.get('Battery_Capacity_Useable')}kWh")
    
    return battery_id

def get_or_create_vehicle_model(cursor, vehicle: Dict[str, Any], make_id: str, oem_id: str, battery_id: str) -> None:
    """Get or create a Vehicle Model record."""
    vehicle_model = vehicle.get("Vehicle_Model", "unknown")
    type = vehicle.get("Vehicle_Model_Version") or "unknown"
    version = "unknown"

    if vehicle_model.lower() == "zoe":
        # Define the patterns to look for in the type string
        type_patterns = ['q210', 'r240', 'r90', 'r110', 'q90', 'r135']
        
        # Check if any of the patterns exist in the type string
        found_type = None
        for pattern in type_patterns:
            if pattern.lower() in type.lower():
                found_type = pattern.lower()
                break
        
        if found_type:
            # Extract the found pattern as type and the rest as version
            type_parts = type.strip().split()
            # Remove the found pattern from type_parts and join the rest as version
            remaining_parts = [part for part in type_parts if found_type not in part.lower()]
            type = found_type
            version = " ".join(remaining_parts) if remaining_parts else "unknown"
        else:
            # Fallback to original logic if no pattern is found
            type_parts = type.strip().split()
            type = type_parts[0] if type_parts else "unknown"
            version = " ".join(type_parts[1:]) if len(type_parts) > 1 else "unknown"
        
        cursor.execute(
            "SELECT id FROM vehicle_model WHERE model_name = LOWER(%s) AND LOWER(type) = LOWER(%s) AND version = LOWER(%s)",
            (vehicle_model, type, version)
        )
        model_result = cursor.fetchone()
    else:
        cursor.execute(
            "SELECT id FROM vehicle_model WHERE model_name = LOWER(%s) AND LOWER(type) = LOWER(%s)",
            (vehicle_model, type)
        )
        model_result = cursor.fetchone()
    
    if not model_result:
        model_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO vehicle_model (
                id, model_name, type, version, make_id, oem_id, autonomy, warranty_date, warranty_km, source, battery_id
            ) VALUES (%s, LOWER(%s), LOWER(%s), LOWER(%s), %s, %s, %s, %s, %s, %s, %s)
        """, (
            model_id,
            vehicle_model,
            type,
            version,
            make_id,
            get_oem(cursor, make_id),
            vehicle.get("Range_WLTP"),
            vehicle.get("Battery_Warranty_Period"),
            vehicle.get("Battery_Warranty_Mileage"),
            vehicle.get("EVDB_Detail_URL"),
            battery_id
        ))
        print(f"Created new vehicle model: {vehicle_model} {type} {version}")
    else:
        cursor.execute("""
            UPDATE vehicle_model 
            SET autonomy = COALESCE(%s, autonomy),
                warranty_date = COALESCE(%s, warranty_date),
                warranty_km = COALESCE(%s, warranty_km),
                source = COALESCE(%s, source)
            WHERE id = %s
        """, (vehicle.get("Range_WLTP"), vehicle.get("Battery_Warranty_Period"), 
              vehicle.get("Battery_Warranty_Mileage"), vehicle.get("EVDB_Detail_URL"), 
              model_result[0]))
        print(f"Updated existing vehicle model: {vehicle_model} {type} {version}")

def process_vehicle(cursor, vehicle: Dict[str, Any]) -> None:
    """Process a single vehicle record."""
    try:
        if vehicle.get("Vehicle_Make", "").lower() == "tesla":
            print(f"Skipping Tesla model: {vehicle.get('Vehicle_Model')} {vehicle.get('Vehicle_Model_Version')}")
            return

        oem_id = get_oem(cursor, vehicle.get("Vehicle_Make", ""))
        make_id = get_or_create_make(cursor, vehicle.get("Vehicle_Make", ""), oem_id)
        battery_id = get_or_create_battery(cursor, vehicle)
        get_or_create_vehicle_model(cursor, vehicle, make_id, battery_id)
        
    except Exception as e:
        print(f"Error processing vehicle {vehicle.get('Vehicle_Model')}: {str(e)}")

def fetch_ev_data():
    """Main function to fetch and process EV data."""
    url = os.getenv("EV_DATABASE_URL")
    print(f"Fetching data from URL: {url}")
    
    data = fetch_api_data(url)
    if not data:
        return None
        
    with get_connection() as con:
        cursor = con.cursor()
        for vehicle in data:
            process_vehicle(cursor, vehicle)
        con.commit()
        print("Successfully processed all vehicle models")
    
    return data

if __name__ == "__main__":
    fetch_ev_data()

