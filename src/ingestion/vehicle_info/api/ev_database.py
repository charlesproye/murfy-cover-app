import requests
import json
import uuid
from core.sql_utils import get_connection
import logging

def fetch_ev_data():
    url = "https://ev-database.org/export_v31/m1/bev/19052863/mji92WSoafoOEIF3lvEB1v"
    
    try:
        # Make the GET request
        response = requests.get(url)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        with get_connection() as con:
            cursor = con.cursor()
            
            for vehicle in data:
                try:
                    # Get or create OEM
                    cursor.execute(
                        "SELECT id FROM oem WHERE LOWER(oem_name) = LOWER(%s)",
                        (vehicle.get("Vehicle_Make", ""),)
                    )
                    oem_result = cursor.fetchone()
                    
                    if not oem_result:
                        oem_id = str(uuid.uuid4())
                        cursor.execute(
                            "INSERT INTO oem (id, oem_name) VALUES (%s, LOWER(%s)) RETURNING id",
                            (oem_id, vehicle.get("Vehicle_Make",""))
                        )
                        oem_id = cursor.fetchone()[0]
                    else:
                        oem_id = oem_result[0]
                    
                    # Skip if OEM is Tesla
                    if vehicle.get("Vehicle_Make", "").lower() == "tesla":
                        logging.info(f"Skipping Tesla model: {vehicle.get('Vehicle_Model')} {vehicle.get('Vehicle_Model_Version')}")
                        continue
                    
                    # Get or create Make
                    cursor.execute(
                        "SELECT id FROM make WHERE LOWER(make_name) = LOWER(%s)",
                        (vehicle.get("Vehicle_Make", ""),)
                    )
                    make_result = cursor.fetchone()
                    
                    if not make_result:
                        make_id = str(uuid.uuid4())
                        cursor.execute(
                            "INSERT INTO make (id, make_name, oem_id) VALUES (%s, LOWER(%s), %s) RETURNING id",
                            (make_id, vehicle.get("Vehicle_Make", ""), oem_id)
                        )
                        make_id = cursor.fetchone()[0]
                    else:
                        make_id = make_result[0]
                    
                    # Check if model already exists
                    cursor.execute(
                        "SELECT id FROM vehicle_model WHERE LOWER(model_name) = LOWER(%s) AND LOWER(type) = LOWER(%s)",
                        (vehicle.get("Vehicle_Model", "unknown"), vehicle.get("Vehicle_Model_Version", "unknown"))
                    )
                    model_result = cursor.fetchone()
                    
                    if not model_result:
                        # Create new model
                        model_id = str(uuid.uuid4())
                        cursor.execute("""
                            INSERT INTO vehicle_model (
                                id, model_name, type, make_id, oem_id, autonomy, warranty_date, warranty_km
                            ) VALUES (%s, LOWER(%s), LOWER(%s), %s, %s, %s, %s, %s)
                        """, (
                            model_id,
                            vehicle.get("Vehicle_Model", "unknown"),
                            vehicle.get("Vehicle_Model_Version", "unknown"),
                            make_id,
                            oem_id,
                            vehicle.get("Range_WLTP"),
                            vehicle.get("Battery_Warranty_Period"),
                            vehicle.get("Battery_Warranty_Mileage")
                        ))
                        logging.info(f"Created new vehicle model: {vehicle.get('Vehicle_Model')} {vehicle.get('Vehicle_Model_Version')}")
                    else:
                        # Update existing model
                        cursor.execute("""
                            UPDATE vehicle_model 
                            SET autonomy = COALESCE(%s, autonomy),
                                warranty_date = COALESCE(%s, warranty_date),
                                warranty_km = COALESCE(%s, warranty_km)
                            WHERE id = %s
                        """, (vehicle.get("Range_WLTP"), vehicle.get("Battery_Warranty_Period"), vehicle.get("Battery_Warranty_Mileage"), model_result[0]))
                        logging.info(f"Updated existing vehicle model: {vehicle.get('Vehicle_Model')} {vehicle.get('Vehicle_Model_Version')}")
                    
                except Exception as e:
                    logging.error(f"Error processing vehicle {vehicle.get('Vehicle_Model')}: {str(e)}")
                    continue
            
            con.commit()
            logging.info("Successfully processed all vehicle models")
            
        return data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making the request: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        return None

if __name__ == "__main__":
    fetch_ev_data()

