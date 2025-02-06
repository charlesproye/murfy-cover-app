import asyncio
import logging
import pandas as pd
import uuid
import aiohttp
import os
import re
import json
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import requests

from core.sql_utils import get_connection
from ingestion.vehicle_info.fleet_info import read_fleet_info as fleet_info
from ingestion.vehicle_info.config import *
from dotenv import load_dotenv
from ingestion.vehicle_info.utils.google_sheets_utils import get_google_client
from ingestion.bmw.api import BMWApi
from ingestion.high_mobility.api import HMApi

load_dotenv()

# Initialize API clients
bmw_api = BMWApi(
    auth_url=os.getenv('BMW_AUTH_URL'),
    base_url=os.getenv('BMW_BASE_URL'),
    client_id=os.getenv('BMW_CLIENT_ID'),
    fleet_id=os.getenv('BMW_FLEET_ID'),
    client_username=os.getenv('BMW_CLIENT_USERNAME'),
    client_password=os.getenv('BMW_CLIENT_PASSWORD')
)

hm_api = HMApi(
    base_url=os.getenv('HM_BASE_URL'),
    client_id=os.getenv('HM_CLIENT_ID'),
    client_secret=os.getenv('HM_CLIENT_SECRET')
)

MAKE_MAPPING = {
    'mercedes-benz': 'mercedes',
    'mercedes': 'mercedes',
    'Mercedes': 'mercedes',
    'Mercedes-Benz': 'mercedes',
    'MERCEDES': 'mercedes',
    'MERCEDES-BENZ': 'mercedes'
}

OEM_MAPPING = {
    'Mercedes': 'mercedes',
    'MERCEDES': 'mercedes',
    'mercedes-benz': 'mercedes',
    'Mercedes-Benz': 'mercedes',
    'MERCEDES-BENZ': 'mercedes',
    'Mercedes': 'mercedes',
    'Stellantis': 'stellantis',
    'STELLANTIS': 'stellantis',
    'stellantis': 'stellantis'
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RATE_LIMIT_DELAY = 0.5
MAX_RETRIES = 3
SPREADSHEET_ID = "1zGwSY41eN00YQbaNf9HNk3g5g6KQaAD1FY7-XS8Uf9w"

def convert_date_format(date_str):
    """Convertit les différents formats de date en format YYYY-MM-DD"""
    if pd.isna(date_str):
        return None
    try:
        # Si c'est un Timestamp pandas
        if isinstance(date_str, pd.Timestamp):
            return date_str.strftime('%Y-%m-%d')
            
        # Si c'est une string
        date_str = str(date_str).split()[0]  # Prend seulement la partie date
        
        if '.' in date_str:  # Format DD.MM.YYYY
            day, month, year = date_str.split('.')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        elif '/' in date_str:  # Format MM/DD/YYYY
            month, day, year = date_str.split('/')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        elif '-' in date_str:  # Format DD-MM-YYYY ou YYYY-MM-DD
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # Si c'est déjà au format YYYY-MM-DD
                return date_str
            else:  # Format DD-MM-YYYY
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            logging.warning(f"Format de date non reconnu: {date_str}")
            return None
            
    except Exception as e:
        logging.warning(f"Format de date invalide: {date_str}, erreur: {str(e)}")
        return None

def standardize_model_type(model: str, type_value: str, make: str) -> tuple[str, str]:
    """Standardise le modèle et le type du véhicule"""
    if not type_value or type_value == 'x':
        return model.lower(), None

    model = model.lower()
    type_value = type_value.lower() if type_value else None
    make_lower = make.lower()
    
    # Nettoyage basique du type
    if type_value:
        for suffix in suffixes_to_remove:
            type_value = type_value.replace(f" {suffix}", "")
    
    if make_lower in mappings and model in mappings[make_lower]:
        model_info = mappings[make_lower][model]
        
        # Applique le nettoyage du modèle si spécifié
        if 'model_clean' in model_info:
            model = model_info['model_clean'](model)
            
        # Applique les patterns pour le type
        if type_value:
            for pattern, replacement in model_info['patterns']:
                if re.search(pattern, type_value):
                    return model.lower(), str(replacement).lower()

    return model.lower(), type_value.strip().lower() if type_value else None

async def check_bmw_activation(session: aiohttp.ClientSession, vin: str) -> bool:
    """Check if a BMW vehicle is actually activated by calling BMW's API"""
    try:
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bmw_api.check_vehicle_status(vin)
        )
        if status_code == 200:
            return True  # Si on peut accéder au véhicule, il est actif
        elif status_code == 404:
            return False
        else:
            logging.error(f"Failed to check BMW activation for {vin}: status {status_code}")
            return False
    except Exception as e:
        logging.error(f"Error checking BMW activation for {vin}: {str(e)}")
        return False

async def check_high_mobility_activation(session: aiohttp.ClientSession, vin: str) -> bool:
    """Check if a High Mobility vehicle is actually activated by calling HM's API"""
    try:
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: hm_api.get_clearance(vin)
        )
        if status_code == 200:
            return result.clearance_status == "ACTIVE"
        elif status_code == 404:
            return False
        else:
            logging.error(f"Failed to check HM activation for {vin}: status {status_code}")
            return False
    except Exception as e:
        logging.error(f"Error checking HM activation for {vin}: {str(e)}")
        return False

async def deactivate_bmw(session: aiohttp.ClientSession, vin: str) -> bool:
    """Deactivate a BMW vehicle using BMW's API"""
    try:
        logging.info(f"Starting BMW deactivation process for VIN: {vin}")
        
        # First check if vehicle exists
        logging.info(f"Checking if BMW vehicle {vin} exists...")
        status_code_first_check, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bmw_api.check_vehicle_status(vin)
        )
        logging.info(f"Vehicle status check returned code: {status_code_first_check}")
        
        if status_code_first_check == 404:
            logging.info(f"BMW vehicle {vin} already deactivated (404 Not Found)")
            return True
            
        if status_code_first_check != 200:
            logging.error(f"Failed to check BMW vehicle {vin} status: {status_code_first_check}")
            if isinstance(result, dict):
                logging.error(f"Error details: {json.dumps(result, indent=2)}")
            else:
                logging.error(f"Error response: {result}")
            return False
            
        # Then delete if it exists
        logging.info(f"Vehicle exists, proceeding with deactivation for VIN: {vin}")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bmw_api.delete_clearance(vin)
        )
        logging.info(f"Deactivation request returned status code: {status_code}")
        
        success = status_code in [200, 204]
        if success:
            logging.info(f"Successfully deactivated BMW vehicle {vin}")
        else:
            logging.error(f"Failed to deactivate BMW vehicle {vin}: status {status_code}")
            if isinstance(result, dict):
                logging.error(f"Error details: {json.dumps(result, indent=2)}")
            else:
                logging.error(f"Error response: {result}")
        return success
    except Exception as e:
        logging.error(f"Exception during BMW deactivation for VIN {vin}: {str(e)}")
        return False

async def deactivate_high_mobility(session: aiohttp.ClientSession, vin: str) -> bool:
    """Deactivate a vehicle using High Mobility's API"""
    try:
        # First check vehicle status
        logging.info(f"Checking High Mobility vehicle {vin} status...")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: hm_api.get_status(vin)
        )
        
        if status_code == 404:
            logging.info(f"High Mobility vehicle {vin} not found (already deactivated)")
            return True
            
        if status_code != 200:
            logging.error(f"Failed to check High Mobility vehicle {vin} status: {status_code}")
            if isinstance(result, dict):
                logging.error(f"Error details: {json.dumps(result, indent=2)}")
            return False
            
        # Check current status
        current_status = result.get('status', '').lower()
        if current_status in ['revoked', 'rejected']:
            logging.info(f"High Mobility vehicle {vin} already deactivated (status: {current_status})")
            return True
            
        if current_status == 'pending':
            logging.info(f"High Mobility vehicle {vin} is pending, proceeding with deactivation")
            
        # Then delete if it exists and is not already deactivated
        logging.info(f"Proceeding with deactivation for VIN: {vin}")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: hm_api.delete_clearance(vin)
        )
        
        success = status_code in [200, 204]
        if success:
            logging.info(f"Successfully deactivated High Mobility vehicle {vin}")
        else:
            logging.error(f"Failed to deactivate High Mobility vehicle {vin}: status {status_code}")
            if isinstance(result, dict):
                logging.error(f"Error details: {json.dumps(result, indent=2)}")
            else:
                logging.error(f"Error response: {result}")
        return success
    except Exception as e:
        logging.error(f"Error deactivating High Mobility vehicle {vin}: {str(e)}")
        return False

async def update_google_sheet_status(vin: str, real_activation: bool, error_message: Optional[str] = None):
    """Update Real Activation and Activation Error columns in Google Sheet"""
    try:
        client = get_google_client()
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        
        # Find the row with the VIN
        cell = sheet.find(vin)
        if not cell:
            logging.error(f"VIN {vin} not found in Google Sheet")
            return False
            
        # Get headers to find column indices
        headers = sheet.row_values(1)
        real_activation_col = headers.index("Real Activation") + 1
        error_col = headers.index("Activation Error") + 1
        
        # Update Real Activation status
        sheet.update_cell(cell.row, real_activation_col, str(real_activation))
        
        # Update error message if provided
        if error_message:
            sheet.update_cell(cell.row, error_col, error_message)
        else:
            sheet.update_cell(cell.row, error_col, "")
            
        logging.info(f"Updated Google Sheet for VIN {vin}: Real Activation={real_activation}, Error={error_message or 'None'}")
        return True
        
    except Exception as e:
        logging.error(f"Error updating Google Sheet for VIN {vin}: {str(e)}")
        return False

async def activate_bmw(session: aiohttp.ClientSession, vin: str) -> tuple[bool, Optional[str]]:
    """Activate a BMW vehicle using BMW's API"""
    try:
        from ingestion.bmw.vehicle import Vehicle
        
        # First check if vehicle is already activated
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bmw_api.get_clearance(vin)
        )
        if status_code == 200:
            if result.clearance_status == "ACTIVE":
                return True, None
            else:
                logging.info(f"BMW vehicle {vin} exists but not active, will reactivate")
        elif status_code != 404:
            return False, f"Failed to check vehicle status: HTTP {status_code}"
            
        # Create new clearance
        vehicle = Vehicle(vin=vin, brand="BMW")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: bmw_api.create_clearance([vehicle])
        )
        if status_code in [200, 201, 204]:
            return True, None
        else:
            error_msg = f"Failed to activate BMW vehicle: HTTP {status_code}"
            return False, error_msg
    except Exception as e:
        error_msg = f"Error activating BMW vehicle: {str(e)}"
        logging.error(error_msg)
        return False, error_msg

async def activate_high_mobility(session: aiohttp.ClientSession, vin: str) -> tuple[bool, Optional[str]]:
    """Activate a vehicle using High Mobility's API"""
    try:
        from ingestion.high_mobility.vehicle import Vehicle
        
        # First check if vehicle is already activated
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: hm_api.get_clearance(vin)
        )
        if status_code == 200:
            if result.clearance_status == "ACTIVE":
                return True, None
            else:
                logging.info(f"High Mobility vehicle {vin} exists but not active, will reactivate")
        elif status_code != 404:
            return False, f"Failed to check vehicle status: HTTP {status_code}"
            
        # Create new clearance
        vehicle = Vehicle(vin=vin, brand="High Mobility")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: hm_api.create_clearance([vehicle])
        )
        if status_code in [200, 201, 204]:
            return True, None
        else:
            error_msg = f"Failed to activate High Mobility vehicle: HTTP {status_code}"
            return False, error_msg
    except Exception as e:
        error_msg = f"Error activating High Mobility vehicle: {str(e)}"
        logging.error(error_msg)
        return False, error_msg

def validate_vehicle_data(vehicle: pd.Series) -> tuple[bool, str]:
    """Valide les données d'un véhicule avant traitement"""
    required_fields = ['vin', 'make', 'model', 'oem', 'owner', 'country']
    
    # Vérifier les champs requis
    for field in required_fields:
        if field not in vehicle or pd.isna(vehicle[field]):
            return False, f"Champ requis manquant: {field}"
            
    # Vérifier le format du VIN
    if not isinstance(vehicle['vin'], str) or len(vehicle['vin']) < 10:
        return False, f"Format de VIN invalide: {vehicle['vin']}"
        
    # Vérifier que les champs texte sont bien des strings
    string_fields = ['make', 'model', 'oem', 'owner', 'country', 'licence_plate']
    for field in string_fields:
        if field in vehicle and not pd.isna(vehicle[field]):
            if not isinstance(vehicle[field], str):
                return False, f"Le champ {field} doit être une chaîne de caractères"
                
    return True, ""

async def process_vehicles(df: pd.DataFrame):
    """Traite les véhicules du DataFrame et les insère dans la base de données"""
    logging.info(f"Début du traitement de {len(df)} véhicules")
    processed_count = 0
    error_count = 0
    skipped_count = 0
    
    COUNTRY_MAPPING = {
        'NL': 'Netherlands',
        'FR': 'France',
        'BE': 'Belgium',
        'DE': 'Germany',
        'LU': 'Luxembourg',
        'ES': 'Spain',
        'IT': 'Italy',
        'PT': 'Portugal',
        'GB': 'United Kingdom',
        'UK': 'United Kingdom'
    }

    # Configuration du timeout pour les appels API
    timeout = aiohttp.ClientTimeout(total=30)  # 30 secondes de timeout total
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        with get_connection() as con:
            # Désactiver l'autocommit pour gérer manuellement les transactions
            con.autocommit = False
            cursor = con.cursor()
            
            # Créer un point de sauvegarde initial
            cursor.execute("SAVEPOINT initial_state")
            
            for index, vehicle in df.iterrows():
                try:
                    logging.info(f"Traitement du véhicule {index + 1}/{len(df)} - VIN: {vehicle['vin']}")
                    
                    # Créer un point de sauvegarde pour chaque véhicule
                    cursor.execute(f"SAVEPOINT vehicle_{index}")
                    
                    # Valider les données avant traitement
                    is_valid, error_message = validate_vehicle_data(vehicle)
                    if not is_valid:
                        logging.warning(f"Véhicule ignoré: {error_message}")
                        skipped_count += 1
                        continue

                    # Handle activation status for BMW and High Mobility vehicles
                    make_lower = vehicle['make'].lower()
                    is_activated = str(vehicle.get('activation', 'FALSE')).upper() == 'TRUE'
                    real_activation = str(vehicle.get('real_activation', '')).upper()
                    logging.info(f"Processing vehicle activation - Make: {make_lower}, Activation status: {is_activated}, Real Activation: {real_activation}")
                    
                    # Skip API calls if Real Activation is already set
                    if real_activation:
                        logging.info(f"Skipping API calls for VIN {vehicle['vin']} as Real Activation is already set to: {real_activation}")
                        vehicle['activation_status'] = str(real_activation == 'TRUE')
                        continue
                    
                    if make_lower == 'bmw':
                        try:
                            logging.info("Processing BMW vehicle")
                            if is_activated:
                                # Try to activate with timeout
                                success, error_msg = await asyncio.wait_for(
                                    activate_bmw(session, vehicle['vin']),
                                    timeout=20.0  # 20 secondes timeout
                                )
                                vehicle['activation_status'] = str(success)
                                await update_google_sheet_status(vehicle['vin'], success, error_msg)
                            else:
                                logging.info("Deactivating BMW vehicle")
                                # Try to deactivate with timeout
                                success = await asyncio.wait_for(
                                    deactivate_bmw(session, vehicle['vin']),
                                    timeout=20.0
                                )
                                error_msg = None if success else "Failed to deactivate BMW vehicle"
                                vehicle['activation_status'] = str(not success)
                                await update_google_sheet_status(vehicle['vin'], not success, error_msg)
                        except Exception as e:
                            error_msg = f"Error processing BMW vehicle: {str(e)}"
                            logging.error(f"Error processing BMW vehicle {vehicle['vin']}: {str(e)}")
                            vehicle['activation_status'] = 'false'
                            await update_google_sheet_status(vehicle['vin'], False, error_msg)
                            
                    elif make_lower in ['ford', 'mercedes', 'kia']:  # High Mobility makes
                        try:
                            if is_activated:
                                logging.info("Activating High Mobility vehicle")
                                # Try to activate with timeout
                                success, error_msg = await asyncio.wait_for(
                                    activate_high_mobility(session, vehicle['vin']),
                                    timeout=20.0
                                )
                                vehicle['activation_status'] = str(success)
                                await update_google_sheet_status(vehicle['vin'], success, error_msg)
                            else:
                                logging.info("Deactivating High Mobility vehicle")
                                # Try to deactivate with timeout
                                success = await asyncio.wait_for(
                                    deactivate_high_mobility(session, vehicle['vin']),
                                    timeout=20.0
                                )
                                error_msg = None if success else "Failed to deactivate High Mobility vehicle"
                                vehicle['activation_status'] = str(not success)
                                await update_google_sheet_status(vehicle['vin'], not success, error_msg)
                        except Exception as e:
                            error_msg = f"Error processing High Mobility vehicle: {str(e)}"
                            logging.error(f"Error processing High Mobility vehicle {vehicle['vin']}: {str(e)}")
                            vehicle['activation_status'] = 'false'
                            await update_google_sheet_status(vehicle['vin'], False, error_msg)
                    else:
                        logging.info(f"Vehicle brand not supported for activation: {make_lower}")
                        vehicle['activation_status'] = 'false'  # Marquer comme non activé pour les marques non supportées
                        await update_google_sheet_status(vehicle['vin'], False, "Brand not supported for activation")

                    # Gestion de l'oem
                    oem_raw = vehicle['oem']
                    oem_lower = OEM_MAPPING.get(oem_raw, oem_raw.lower())
                    
                    cursor.execute("""
                        SELECT id FROM oem
                        WHERE LOWER(oem_name) = %s
                    """, (oem_lower,))
                    
                    oem_result = cursor.fetchone()
                    
                    if not oem_result:
                        logging.error(f"OEM non trouvé pour la marque: {vehicle['oem']} (mappé à {oem_lower})")
                        cursor.execute(f"ROLLBACK TO SAVEPOINT vehicle_{index}")
                        error_count += 1
                        continue
                    oem_id = oem_result[0]      

                    # Gestion de la marque
                    make_raw = vehicle['make']
                    make_lower = MAKE_MAPPING.get(make_raw, make_raw.lower())
                    
                    cursor.execute("""
                        SELECT id FROM make 
                        WHERE LOWER(make_name) = %s
                    """, (make_lower,))
                    
                    make_result = cursor.fetchone()
                    
                    if not make_result:
                        logging.error(f"MAKE non trouvé pour la marque: {vehicle['make']} (mappé à {make_lower})")
                        cursor.execute(f"ROLLBACK TO SAVEPOINT vehicle_{index}")
                        error_count += 1
                        continue
                    make_id = make_result[0]

                    # Standardisation du modèle et type
                    model_name = vehicle['model'].strip() if pd.notna(vehicle['model']) else None
                    type_value = vehicle['type'].strip() if pd.notna(vehicle['type']) else None
                    if not model_name:
                        logging.error(f"Modèle manquant pour le véhicule VIN: {vehicle['vin']}")
                        cursor.execute(f"ROLLBACK TO SAVEPOINT vehicle_{index}")
                        error_count += 1
                        continue

                    model_name, type_value = standardize_model_type(model_name, type_value, vehicle['make'])

                    # Recherche du modèle dans la base
                    cursor.execute("""
                        SELECT id FROM vehicle_model 
                        WHERE LOWER(model_name) = %s 
                        AND (
                            (LOWER(type) = %s AND %s IS NOT NULL)
                            OR (type IS NULL AND %s IS NULL)
                        )
                        AND make_id = %s AND oem_id = %s
                    """, (model_name.lower(), type_value.lower() if type_value else None, 
                            type_value, type_value, make_id, oem_id))
                    
                    result = cursor.fetchone()
                    if result:
                        vehicle_model_id = result[0]
                    else:
                        
                        vehicle_model_id = str(uuid.uuid4())
                        if type_value:
                            cursor.execute("""
                                INSERT INTO vehicle_model (id, model_name, type, make_id,oem_id)
                                VALUES (%s, %s, %s, %s,%s)
                                RETURNING id
                            """, (
                                vehicle_model_id,
                                model_name.lower(),
                                type_value.lower(),
                                make_id,
                                oem_id
                            ))
                        else:
                            cursor.execute("""
                                INSERT INTO vehicle_model (id, model_name, make_id,oem_id)
                                VALUES (%s, %s, %s,%s)
                                RETURNING id
                            """, (
                                vehicle_model_id,
                                model_name.lower(),
                                make_id,
                                oem_id
                            ))
                        vehicle_model_id = cursor.fetchone()[0]
                    
                        logging.info(f"Créé nouveau modèle: {model_name} {type_value or ''} pour {vehicle['make']} du contructeur {vehicle['oem']}") 
                    # Récupération fleet_id
                    cursor.execute("""
                        SELECT id FROM fleet 
                        WHERE LOWER(fleet_name) = LOWER(%s)
                    """, (vehicle['owner'],))
                    fleet_result = cursor.fetchone()
                    if not fleet_result:
                        logging.error(f"Fleet non trouvée pour owner: {vehicle['owner']}")
                        continue
                    fleet_id = fleet_result[0]
            
                    # Gestion de la région
                    if pd.isna(vehicle['country']):
                        logging.warning(f"Pays manquant pour le véhicule VIN: {vehicle['vin']}")
                        continue
                    
                    country = COUNTRY_MAPPING.get(vehicle['country'], vehicle['country'])
                    cursor.execute("""
                        SELECT id FROM region 
                        WHERE LOWER(region_name) = LOWER(%s)
                    """, (country,))
                    region_result = cursor.fetchone()
                    if not region_result:
                        region_id = str(uuid.uuid4())
                        cursor.execute("""
                            INSERT INTO region (id, region_name)
                            VALUES (%s, %s)
                            RETURNING id
                        """, (region_id, country))
                        region_id = cursor.fetchone()[0]
                        logging.info(f"Nouvelle région créée: {country}")
                    else:
                        region_id = region_result[0]
                    # Gestion du véhicule
                    cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vehicle['vin'],))
                    vehicle_exists = cursor.fetchone()
                    
                    end_of_contract = convert_date_format(vehicle['end_of_contract'])
                    start_date = convert_date_format(vehicle['start_date']) 
                    if vehicle_exists:
                        cursor.execute("""
                            UPDATE vehicle 
                            SET fleet_id = %s,
                                region_id = %s,
                                vehicle_model_id = %s,
                                licence_plate = %s,
                                end_of_contract_date = %s,
                                start_date = %s,
                                activation_status = %s
                            WHERE vin = %s
                        """, (
                            fleet_id,
                            region_id,
                            vehicle_model_id,
                            vehicle['licence_plate'],
                            end_of_contract,
                            start_date,
                            vehicle.get('activation_status'),
                            vehicle['vin']
                        ))
                        logging.info(f"Véhicule mis à jour avec VIN: {vehicle['vin']}")
                    else:
                        vehicle_id = str(uuid.uuid4())
                        cursor.execute("""
                            INSERT INTO vehicle (
                                id, vin, fleet_id, region_id, vehicle_model_id,
                                licence_plate, end_of_contract_date, start_date, activation_status
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            vehicle_id,
                            vehicle['vin'],
                            fleet_id,
                            region_id,
                            vehicle_model_id,
                            vehicle['licence_plate'],
                            end_of_contract,
                            start_date,
                            vehicle.get('activation_status')
                        ))
                        logging.info(f"Nouveau véhicule inséré avec VIN: {vehicle['vin']}")
                
                except Exception as e:
                    error_count += 1
                    logging.error(f"Erreur lors du traitement du véhicule {vehicle.get('vin', 'Unknown VIN')}: {str(e)}")
                    logging.error(f"Données du véhicule: {vehicle.to_dict()}")
                    # Revenir au point de sauvegarde du véhicule en cas d'erreur
                    cursor.execute(f"ROLLBACK TO SAVEPOINT vehicle_{index}")
                    continue
                else:
                    processed_count += 1
                    # Libérer le point de sauvegarde du véhicule en cas de succès
                    cursor.execute(f"RELEASE SAVEPOINT vehicle_{index}")
                    if processed_count % 10 == 0:
                        logging.info(f"Progression: {processed_count}/{len(df)} véhicules traités")
                        # Commit intermédiaire tous les 10 véhicules
                        con.commit()
                        cursor.execute("SAVEPOINT initial_state")
            
            try:
                # Commit final
                con.commit()
                logging.info(f"Traitement terminé. {processed_count} véhicules traités avec succès, {error_count} erreurs, {skipped_count} ignorés.")
            except Exception as e:
                logging.error(f"Erreur lors du commit final: {str(e)}")
                con.rollback()
                raise

async def get_existing_model_metadata():
    """Récupère les métadonnées existantes des modèles de véhicules"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                m.make_name,
                vm.model_name,
                vm.type,
                vm.url_image,
                vm.warranty_km,
                vm.warranty_date,
                vm.capacity
            FROM vehicle_model vm
            JOIN make m ON vm.make_id = o.id
            WHERE vm.url_image IS NOT NULL 
               OR vm.warranty_km IS NOT NULL 
               OR vm.warranty_date IS NOT NULL 
               OR vm.capacity IS NOT NULL
            ORDER BY m.make_name, vm.model_name, vm.type
        """)
        
        results = cursor.fetchall()
        
        if results:
            print("\nMétadonnées existantes des modèles :")
            print("--------------------------------------------------------------------------------")
            print("Marque | Modèle | Type | URL | Garantie km | Garantie années | Capacité")
            print("--------------------------------------------------------------------------------")
            for row in results:
                make, model, type_value, url, warranty_km, warranty_date, capacity = row
                print(f"{make} | {model} | {type_value or 'N/A'} | {url or 'N/A'} | {warranty_km or 'N/A'} | {warranty_date or 'N/A'} | {capacity or 'N/A'}")
            print("--------------------------------------------------------------------------------")
        else:
            print("Aucune métadonnée trouvée dans la base")
            
        return results
            
async def main(df: pd.DataFrame):
    try:
        logging.info(f"Nombre total de véhicules dans fleet_info: {len(df)}")
        df = df.query("make != 'tesla'")
        logging.info(f"Nombre de véhicules non-Tesla à traiter: {len(df)}")

        await process_vehicles(df)
        #metadata = await get_existing_model_metadata()
        
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")
        raise  # Relancer l'exception pour ne pas masquer l'erreur

if __name__ == "__main__":
    df = asyncio.run(fleet_info(owner_filter="Ayvens"))
    asyncio.run(main(df))



