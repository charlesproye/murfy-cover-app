import asyncio
import aiohttp
import json
import os
import logging
from typing import List, Dict
from core.sql_utils import con
import uuid
import pandas as pd
import time
import re

from core.sql_utils import get_connection
from ingestion.vehicle_info.fleet_info import read_fleet_info as fleet_info
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

current_date = datetime.now().strftime('%Y-%m-%d')
current_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(current_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f'tesla_errors_{current_date}.log')),
        logging.StreamHandler()  # Garde aussi l'affichage console
    ]
)

current_date = datetime.now().strftime('%Y-%m-%d')
current_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(current_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)

# Handler pour les erreurs (fichier)
error_handler = logging.FileHandler(os.path.join(log_dir, f'tesla_errors_{current_date}.log'))
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Handler pour la console (info)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configuration du logger
logger = logging.getLogger('tesla_logger')
logger.setLevel(logging.INFO)
logger.addHandler(error_handler)
logger.addHandler(console_handler)

ACCOUNT_TOKEN_KEYS = {
    'OLINO': 'ACCESS_TOKEN_OLINO',
    'AYVENS_SLBV': 'ACCESS_TOKEN_AYVENS_SLBV',
    'AYVENS_BLBV': 'ACCESS_TOKEN_AYVENS_BLBV',
    'AYVENS': 'ACCESS_TOKEN_AYVENS',
    'AYVENS_NV': 'ACCESS_TOKEN_AYVENS_NV',
    'AYVENS_NVA': 'ACCESS_TOKEN_AYVENS_NVA'
}

RATE_LIMIT_DELAY = 0.5  
MAX_RETRIES = 3 

TESLA_PATTERNS = {
    'model 3': {
        'patterns': [
            (r'.*standard range.*plus.*rear.?wheel.*|.*standard range.*plus.*rwd.*|.*rear.?wheel drive.*', 'RWD'),
            (r'.*performance.*dual motor.*|.*performance.*', 'Performance'),
            (r'.*long range.*all.?wheel drive.*', 'Long Range AWD'),
        ]
    },
    'model s': {
        'patterns': [
            (r'.*100d.*', '100D'),
            (r'.*75d.*', '75D'),
            (r'.*long range.*plus.*', 'Long Range Plus'),
            (r'.*long range.*', 'Long Range'),
            (r'.*plaid.*', 'Plaid'),
            (r'.*performance.*', 'Performance'),
            (r'.*standard range.*', 'Standard Range'),
        ]
    },
    'model x': {
        'patterns': [
            (r'.*long range.*plus.*', 'Long Range Plus'),
            (r'.*long range.*', 'Long Range'),
        ]
    },
    'model y': {
        'patterns': [
            (r'.*long range.*rwd.*', 'Long Range RWD'),
            (r'.*long range.*all.?wheel drive.*', 'Long Range AWD'),
            (r'.*performance.*awd.*', 'Performance'),
            (r'.*rear.?wheel drive.*', 'RWD'),
        ]
    }
}

async def fetch_slack_messages(session: aiohttp.ClientSession, channel_id: str, slack_token: str) -> List[Dict]:
    """Récupère les messages de Slack"""
    url = f"https://slack.com/api/conversations.history?channel={channel_id}"
    headers = {
        'Authorization': f'Bearer {slack_token}',
        'Content-Type': 'application/json'
    }
    
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            if data.get('ok'):
                return data.get('messages', [])
            else:
                logging.error(f"Error fetching messages: {data.get('error')}")
                return []
        else:
            logging.error(f"Failed to fetch messages: HTTP {response.status}")
            return []

async def get_token_from_slack(session: aiohttp.ClientSession, access_token_key: str) -> str:
    """Récupère le token depuis Slack"""
    try:
        logging.info(f"Getting token for {access_token_key} from Slack...")
        slack_token = os.getenv('SLACK_TOKEN')
        channel_id = 'C0816LXFCNL'
        
        if not slack_token:
            logging.error("Slack bot token is not set")
            return None
            
        messages = await fetch_slack_messages(session, channel_id, slack_token)
        
        for i in range(6):
            try:
                message_text = messages[i]['blocks'][0]['elements'][0]['elements'][3]['text']
                response_key = messages[i]['blocks'][0]['elements'][0]['elements'][0]['text'].split(':')[0]
                response_data = json.loads(message_text)
                
                if response_key == access_token_key:
                    logging.info(f"Found token for {access_token_key}")
                    return response_data['access_token']
            except (KeyError, IndexError, json.JSONDecodeError):
                continue
        
        logging.warning(f"No token found for {access_token_key}")
        return None
        
    except Exception as e:
        logging.error(f"Error getting token from Slack: {str(e)}")
        return None

async def get_account_vins_mapping(session: aiohttp.ClientSession) -> Dict[str, List[str]]:
    """Récupère la liste des VINs pour chaque compte et sauvegarde dans un fichier"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cache_file = os.path.join(current_dir, 'data', 'account_vins_mapping.json')
    
    if os.path.exists(cache_file):
        file_age = time.time() - os.path.getmtime(cache_file)
        if file_age < 24 * 3600:   #24h
            try:
                with open(cache_file, 'r') as f:
                    account_vins = json.load(f)
                logging.info(f"Loaded VIN mapping from cache file")
                return account_vins
            except Exception as e:
                logging.warning(f"Could not load cache file: {str(e)}")
    
    account_vins = {}
    
    for account_name, token_key in ACCOUNT_TOKEN_KEYS.items():
        try:
            access_token = await get_token_from_slack(session, token_key)
            if not access_token:
                logging.error(f"Could not get access token for {account_name}")
                continue
                
            vins = await get_all_vehicles(session, access_token)
            account_vins[account_name] = vins
            logging.info(f"Found {len(vins)} vehicles for account {account_name}")
            
        except Exception as e:
            logging.error(f"Error getting vehicles for account {account_name}: {str(e)}")
    
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(account_vins, f, indent=2)
        logging.info(f"Saved VIN mapping to {cache_file}")
    except Exception as e:
        logging.error(f"Could not save cache file: {str(e)}")
    
    return account_vins

async def get_all_vehicles(session: aiohttp.ClientSession, access_token: str) -> List[str]:
    """Récupère tous les VINs des véhicules pour un token donné"""
    all_vins = []
    page = 1
    retries = 0
    
    while True:
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles?page={page}&per_page=100"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 429:
                    if retries < MAX_RETRIES:
                        retries += 1
                        retry_delay = RATE_LIMIT_DELAY * 2**retries
                        logger.warning(f"Rate limit hit, waiting {retry_delay}s before retry {retries}/{MAX_RETRIES}")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        logger.error("Max retries reached for rate limiting")
                        break
                        
                if response.status != 200:
                    response_text = await response.text()
                    logger.error(f"Error fetching vehicles: HTTP {response.status}\nURL: {url}\nResponse: {response_text}")
                    break
                
                retries = 0
                data = await response.json()
                vehicles = data.get('response', [])
                
                if not vehicles:  # Page vide = on a fini
                    break
                
                vins = [vehicle['vin'] for vehicle in vehicles]
                all_vins.extend(vins)
                logger.info(f"Found {len(vins)} vehicles on page {page}. Total so far: {len(all_vins)}")
                
                # Continue à la page suivante
                page += 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
                
        except Exception as e:
            logger.error(f"Error fetching vehicles: {str(e)}")
            break
    
    logger.info(f"Total vehicles retrieved: {len(all_vins)}")
    return all_vins

async def get_vehicle_options(session: aiohttp.ClientSession, access_token: str, vin: str) -> Dict:
    """Récupère les options pour un VIN donné"""
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/dx/vehicles/options?vin={vin}"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                
                model_info = next(
                    (item for item in data.get('codes', []) if item['code'].startswith('$MT')),
                    None
                )
                
                if model_info:
                    # Garde la version originale (ex: MT301)
                    version = model_info['code'][1:]
                    model_code = version[2]
                    
                    if model_code in ["1", "7"]:
                        model_code = "s"
                    model_name = f"model {model_code}".lower()
                    
                    display_name = model_info['displayName'].lower()
                    vehicle_type = "unknown"
                    
                    if model_name in TESLA_PATTERNS:
                        for pattern, type_name in TESLA_PATTERNS[model_name]['patterns']:
                            if re.match(pattern, display_name, re.IGNORECASE):
                                vehicle_type = type_name
                                break
                else:
                    model_name = "unknown"
                    vehicle_type = "unknown"
                    version = "unknown"
                
                return {
                    'vin': vin,
                    'model_name': model_name,
                    'type': vehicle_type,
                    'version': version
                }
            else:
                logging.error(f"Error fetching options for VIN {vin}: HTTP {response.status}")
                return {'vin': vin, 'model_name': 'unknown', 'type': 'unknown', 'version': 'unknown'}
    except Exception as e:
        logging.error(f"Error fetching options for VIN {vin}: {str(e)}")
        return {'vin': vin, 'model_name': 'unknown', 'type': 'unknown', 'version': 'unknown'}

async def get_start_date(session: aiohttp.ClientSession, access_token: str, vin: str) -> str:
    """Récupère la warranty expiration date et le coverageAgeInYears et fait leur différence pour calculer la start_date"""
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/dx/warranty/details?vin={vin}"
    headers = {'Authorization': f'Bearer {access_token}'}

    retries = 3
    while retries > 0:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    activeWarranty = data.get("activeWarranty")

                    if activeWarranty:
                        warranty = activeWarranty[0]
                        expirationDate = warranty.get("expirationDate")
                        coverageAgeInYears = warranty.get("coverageAgeInYears")

                        if expirationDate and coverageAgeInYears is not None:
                            expiration_date_obj = datetime.fromisoformat(expirationDate.replace("Z", "+00:00"))
                            start_date_obj = expiration_date_obj - timedelta(days=int(coverageAgeInYears * 365.25))
                            return start_date_obj.strftime('%d-%m-%Y')
                    
                    logging.warning(f"No valid warranty data for VIN {vin}")
                    return None
                else:
                    logging.warning(f"HTTP {response.status} error fetching warranty for VIN {vin}, retries left: {retries-1}")
                    retries -= 1  # Wait 2 seconds before retrying

        except Exception as e:
            logging.error(f"Exception getting start date for VIN {vin}: {str(e)}")
            return None

    logging.error(f"Failed to fetch start date for VIN {vin} after 3 retries")
    return None

async def process_account(session: aiohttp.ClientSession, account_name: str, token_key: str, df: pd.DataFrame, account_vins: List[str]) -> List[Dict]:

    def convert_date_format(date_str):
        if pd.isna(date_str):
            return None
        try:
            date_str = date_str.split()[0]
            
            if '.' in date_str:  # Format DD.MM.YYYY
                day, month, year = date_str.split('.')
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            elif '/' in date_str:  # Format MM/DD/YYYY
                month, day, year = date_str.split('/')
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            elif '-' in date_str:  # Format DD-MM-YYYY
                day, month, year = date_str.split('-')
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            else:
                logging.warning(f"Unrecognized date format: {date_str}")
                return None
        except Exception as e:
            logging.warning(f"Invalid date format: {date_str}, error: {str(e)}")
            return None
        
    """Process only VINs that belong to this account"""
    access_token = await get_token_from_slack(session, token_key)
    
    if not access_token:
        logging.error(f"Could not get access token for {account_name}")
        return []
    
    account_df = df[df['vin'].isin(account_vins)]
    vins = account_df['vin'].unique().tolist()
    logging.info(f"Processing {len(vins)} vehicles from fleet info for {account_name}")
    
    for vin in vins:
        with get_connection() as con:
            try:
                cursor = con.cursor()
                vehicle_data = df[df['vin'] == vin].iloc[0]
                options = await get_vehicle_options(session, access_token, vin)
                
                cursor.execute("""
                    SELECT id FROM vehicle_model 
                    WHERE LOWER(model_name) = LOWER(%s) 
                    AND LOWER(type) = LOWER(%s)
                    AND (version IS NULL OR version = '')
                """, (options['model_name'], options['type']))
                
                empty_version_result = cursor.fetchone()
                
                if empty_version_result:
                    vehicle_model_id = empty_version_result[0]
                    cursor.execute("""
                        UPDATE vehicle_model 
                        SET version = %s
                        WHERE id = %s
                    """, (options['version'], vehicle_model_id))
                    logging.info(f"Updated vehicle_model {vehicle_model_id} with version {options['version']}")
                else:
                    cursor.execute("""
                        SELECT id FROM vehicle_model 
                        WHERE LOWER(model_name) = LOWER(%s) 
                        AND LOWER(type) = LOWER(%s)
                        AND version = %s
                    """, (options['model_name'], options['type'], options['version']))
                    
                    result = cursor.fetchone()
                    if result:
                        vehicle_model_id = result[0]
                    else:
                        vehicle_model_id = str(uuid.uuid4())
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, type, version, oem_id, make_id)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            options['model_name'],
                            options['type'],
                            options['version'],
                            '98809ac9-acb5-4cca-b67a-c1f6c489035a',
                            '22d426b6-89bb-422e-a50b-26ecf7473247'
                        ))
                        vehicle_model_id = cursor.fetchone()[0]
                        logging.info(f"Created new vehicle_model for {options['model_name']} {options['type']} version {options['version']}")
                
                cursor.execute("""
                    SELECT id FROM fleet 
                    WHERE LOWER(fleet_name) = LOWER(%s)
                """, (vehicle_data['owner'],))
                
                fleet_result = cursor.fetchone()
                if not fleet_result:
                    logging.error(f"Fleet not found for ownership: {vehicle_data['owner']}")
                    con.rollback()
                    continue
                fleet_id = fleet_result[0]
                
                cursor.execute("""
                    SELECT id FROM region 
                    WHERE LOWER(region_name) = LOWER(%s)
                """, (vehicle_data['country'],))
                
                region_result = cursor.fetchone()
                if not region_result:
                    logging.error(f"Region not found for country: {vehicle_data['country']}")
                    con.rollback()
                    continue
                region_id = region_result[0]
                
                cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vin,))
                vehicle_exists = cursor.fetchone()

                end_of_contract = convert_date_format(vehicle_data['end_of_contract'])
                start_date = await get_start_date(session, access_token,vin)
                start_date = convert_date_format(start_date)
                print(f'start date is {start_date}')
                
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
                        vehicle_data['licence_plate'],
                        end_of_contract,
                        start_date,
                        vehicle_data['activation'],
                        vin
                    ))
                    logging.info(f"Updated vehicle with VIN: {vin}")
                else:
                    vehicle_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO vehicle (
                            id, vin, fleet_id, region_id, vehicle_model_id,
                            licence_plate, end_of_contract_date, start_date, activation_status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        vehicle_id, vin, fleet_id, region_id, vehicle_model_id,
                        vehicle_data['licence_plate'],
                        end_of_contract,
                        start_date,
                        vehicle_data['activation']
                    ))
                    logging.info(f"Inserted new vehicle with VIN: {vin}")
                
                con.commit()
                await asyncio.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                con.rollback()
                logging.error(f"Error processing VIN {vin}: {str(e)}")
                continue
    
    return []


async def main(df: pd.DataFrame):
    try:
        
        async with aiohttp.ClientSession() as session:
            account_vins_mapping = await get_account_vins_mapping(session)
            
            for account_name, token_key in ACCOUNT_TOKEN_KEYS.items():
                try:
                    account_vins = account_vins_mapping.get(account_name, [])
                    if not account_vins:
                        logging.warning(f"No VINs found for account {account_name}")
                        continue
                        
                    await process_account(session, account_name, token_key, df, account_vins)
                except Exception as e:
                    logging.error(f"Error processing account {account_name}: {str(e)}")
                    
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")

async def update_activation_status(df: pd.DataFrame):
    """Update only the activation status of vehicles that have activation=True and owner=Ayvens in the DataFrame."""
    try:
        # Filter DataFrame for Ayvens Tesla vehicles with activation=True
        filtered_df = df[
            (df['activation'] == True) & 
            (df['owner'] == 'Ayvens') &
            (df['oem'] == 'TESLA')
        ]
        logging.info(f"Found {len(filtered_df)} Ayvens Tesla vehicles with activation=True")
        
        if filtered_df.empty:
            logging.warning("No matching vehicles found after filtering")
            return
            
        # Load account_vins_mapping
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.join(current_dir, 'data', 'account_vins_mapping.json')
        
        if not os.path.exists(mapping_file):
            logging.error("account_vins_mapping.json file not found")
            return
            
        with open(mapping_file, 'r') as f:
            account_vins_mapping = json.load(f)
        
        # Process for each Ayvens account in the mapping
        async with aiohttp.ClientSession() as session:
            for account_name, token_key in ACCOUNT_TOKEN_KEYS.items():
                if 'AYVENS' in account_name:  # Process all Ayvens accounts
                    account_vins = account_vins_mapping.get(account_name, [])
                    if not account_vins:
                        logging.warning(f"No VINs found for account {account_name}")
                        continue
                    
                    # Filter DataFrame for this account's VINs
                    account_df = filtered_df[filtered_df['vin'].isin(account_vins)]
                    if account_df.empty:
                        logging.info(f"No matching vehicles found for account {account_name}")
                        continue
                        
                    logging.info(f"Processing {len(account_df)} vehicles for {account_name}")
                    
                    # Process the vehicles using process_account
                    await process_account(
                        session=session,
                        account_name=account_name,
                        token_key=token_key,
                        df=account_df,
                        account_vins=account_vins
                    )
            
    except Exception as e:
        logging.error(f"Error in update_activation_status: {str(e)}")

if __name__ == "__main__":
    df = asyncio.run(fleet_info())
    
    # Update activation status for Ayvens Tesla vehicles
    asyncio.run(update_activation_status(df))
    
    # Or run the full update
    # asyncio.run(main(df))
