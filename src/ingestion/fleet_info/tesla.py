import asyncio
import aiohttp
import json
import os
import logging
from typing import List, Dict
from core.sql_utils import get_connection
import uuid

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Clés des tokens pour différents comptes
ACCOUNT_TOKEN_KEYS = {
    'OLINO': 'ACCESS_TOKEN_OLINO',
    'AYVENS_SLBV': 'ACCESS_TOKEN_AYVENS_SLBV',
    'AYVENS_BLBV': 'ACCESS_TOKEN_AYVENS_BLBV',
    'AYVENS': 'ACCESS_TOKEN_AYVENS',
    'AYVENS_NV': 'ACCESS_TOKEN_AYVENS_NV',
    'AYVENS_NVA': 'ACCESS_TOKEN_AYVENS_NVA'
}

RATE_LIMIT_DELAY = 0.5  # Délai en secondes entre les requêtes
MAX_RETRIES = 3  # Nombre maximum de tentatives en cas d'erreur

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
        
        for i in range(6):  # Vérifie les 6 premiers messages
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

async def get_all_vehicles(session: aiohttp.ClientSession, access_token: str) -> List[str]:
    """Récupère tous les VINs des véhicules pour un token donné"""
    all_vins = []
    page = 1
    retries = 0
    
    while True:
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles?page={page}"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 429:  # Too Many Requests
                    if retries < MAX_RETRIES:
                        retries += 1
                        await asyncio.sleep(RATE_LIMIT_DELAY * 2**retries)  # Exponential backoff
                        continue
                    else:
                        logging.error("Max retries reached for rate limiting")
                        break
                        
                if response.status != 200:
                    logging.error(f"Error fetching vehicles: HTTP {response.status}")
                    break
                
                # Reset retries on successful request
                retries = 0
                data = await response.json()
                vehicles = data.get('response', [])
                
                if not vehicles:
                    break
                
                vins = [vehicle['vin'] for vehicle in vehicles]
                all_vins.extend(vins)
                logging.info(f"Found {len(vins)} vehicles on page {page}")
                
                page += 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
                
        except Exception as e:
            logging.error(f"Error fetching vehicles: {str(e)}")
            break
    
    return all_vins

async def get_vehicle_options(session: aiohttp.ClientSession, access_token: str, vin: str) -> Dict:
    """Récupère les options pour un VIN donné"""
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/dx/vehicles/options?vin={vin}"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                
                # Extraire model_name et type
                model_info = next(
                    (item for item in data.get('codes', []) if item['code'].startswith('$MT')),
                    None
                )
                
                if model_info:
                    model_code = model_info['code'][3]
                    if model_code in ["1", "7"]:
                        model_code = "S"
                    model_name = f"model {model_code}".lower()
                    
                    display_name = model_info['displayName']
                    vehicle_type = display_name.split(f"Model {model_code} ")[-1].lower()
                else:
                    model_name = "unknown"
                    vehicle_type = "unknown"
                
                return {
                    'vin': vin,
                    'model_name': model_name,
                    'type': vehicle_type,
                }
            else:
                logging.error(f"Error fetching options for VIN {vin}: HTTP {response.status}")
                return {'vin': vin, 'model_name': 'unknown', 'type': 'unknown', 'codes': []}
    except Exception as e:
        logging.error(f"Error fetching options for VIN {vin}: {str(e)}")
        return {'vin': vin, 'model_name': 'unknown', 'type': 'unknown', 'codes': []}

async def process_account(session: aiohttp.ClientSession, account_name: str, token_key: str) -> List[Dict]:
    # Récupérer le token depuis Slack
    access_token = await get_token_from_slack(session, token_key)
    if not access_token:
        logging.error(f"Could not get access token for {account_name}")
        return []
    
    # Récupérer tous les VINs
    logging.info(f"Processing account: {account_name}")
    vins = await get_all_vehicles(session, access_token)
    logging.info(f"Found total {len(vins)} vehicles for {account_name}")
    
    # Récupérer les options pour chaque VIN
    options_data = []
    with get_connection() as conn:
        cursor = conn.cursor()
        for vin in vins:
            options = await get_vehicle_options(session, access_token, vin)
            
            # Vérifier si la combinaison model_name/type existe déjà
            cursor.execute("""
                SELECT id FROM vehicle_model 
                WHERE LOWER(model_name) = LOWER(%s) AND LOWER(type) = LOWER(%s)
            """, (options['model_name'], options['type']))
            
            if cursor.fetchone() is not None:
                logging.debug(f"Skipping duplicate model_name/type: {options['model_name']}/{options['type']}")
                continue
            
            # Si on arrive ici, c'est que la combinaison n'existe pas encore
            vehicle_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle_model (id, model_name, type, oem_id)
                VALUES (%s, %s, %s, %s)
            """, (
                vehicle_id,
                options['model_name'],
                options['type'],
                '98809ac9-acb5-4cca-b67a-c1f6c489035a'
            ))
            
            logging.info(f"Added new model/type combination: {options['model_name']}/{options['type']}")
            options_data.append(options)
            await asyncio.sleep(0.5)
        
        conn.commit()
    
    return options_data

async def main():
    all_data = []
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'options.json')
    temp_file_path = file_path + '.tmp'
    
    # Charger les données existantes si disponibles
    if os.path.exists(temp_file_path):
        try:
            with open(temp_file_path, 'r') as f:
                all_data = json.load(f)
            logging.info(f"Loaded {len(all_data)} existing records from temporary file")
        except Exception as e:
            logging.warning(f"Could not load temporary file: {str(e)}")
    
    async with aiohttp.ClientSession() as session:
        for account_name, token_key in ACCOUNT_TOKEN_KEYS.items():
            try:
                account_data = await process_account(session, account_name, token_key)
                all_data.extend(account_data)
                # Sauvegarder les progrès régulièrement
                with open(temp_file_path, 'w') as f:
                    json.dump(all_data, f, indent=2)
            except Exception as e:
                logging.error(f"Error processing account {account_name}: {str(e)}")
    
    # Déplacer le fichier temporaire vers le fichier final
    try:
        os.replace(temp_file_path, file_path)
        logging.info(f"Successfully wrote all data to {file_path}")
    except Exception as e:
        logging.error(f"Error finalizing file: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

