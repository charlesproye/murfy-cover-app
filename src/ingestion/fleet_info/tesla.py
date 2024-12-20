import asyncio
import aiohttp
import json
import os
import logging
from typing import List, Dict
from core.sql_utils import get_connection
import uuid
import pandas as pd
import time
import re
from ingestion.fleet_info.config import *

from dotenv import load_dotenv

load_dotenv()


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
        if file_age < 864000:  # J'ai mis 30 jours
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
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles?page={page}"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 429: #rip
                    if retries < MAX_RETRIES:
                        retries += 1
                        await asyncio.sleep(RATE_LIMIT_DELAY * 2**retries)
                        continue
                    else:
                        logging.error("Max retries reached for rate limiting")
                        break
                        
                if response.status != 200:
                    logging.error(f"Error fetching vehicles: HTTP {response.status}")
                    break
                
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
                
                model_info = next(
                    (item for item in data.get('codes', []) if item['code'].startswith('$MT')),
                    None
                )
                
                if model_info:
                    # Garde la version originale (ex: MT301)
                    version = model_info['code'][1:]
                    model_code = version[2]
                    
                    if model_code in ["1", "7"]:
                        model_code = "S"
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
    
    account_df = df[df['VIN'].isin(account_vins)]
    vins = account_df['VIN'].unique().tolist()
    logging.info(f"Processing {len(vins)} vehicles from fleet info for {account_name}")
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        for vin in vins:
            try:
                vehicle_data = df[df['VIN'] == vin].iloc[0]
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
                            INSERT INTO vehicle_model (id, model_name, type, version, oem_id)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            options['model_name'],
                            options['type'],
                            options['version'],
                            '98809ac9-acb5-4cca-b67a-c1f6c489035a'
                        ))
                        vehicle_model_id = cursor.fetchone()[0]
                        logging.info(f"Created new vehicle_model for {options['model_name']} {options['type']} version {options['version']}")
                
                cursor.execute("""
                    SELECT id FROM fleet 
                    WHERE LOWER(fleet_name) = LOWER(%s)
                """, (vehicle_data['Ownership '],))
                
                fleet_result = cursor.fetchone()
                if not fleet_result:
                    logging.error(f"Fleet not found for ownership: {vehicle_data['Ownership ']}")
                    continue
                fleet_id = fleet_result[0]
                
                cursor.execute("""
                    SELECT id FROM region 
                    WHERE LOWER(region_name) = LOWER(%s)
                """, (vehicle_data['Country'],))
                
                region_result = cursor.fetchone()
                if not region_result:
                    logging.error(f"Region not found for country: {vehicle_data['Country']}")
                    continue
                region_id = region_result[0]
                
                cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vin,))
                vehicle_exists = cursor.fetchone()

                end_of_contract = convert_date_format(vehicle_data['End of Contract'])
                start_date = convert_date_format(vehicle_data['Start Date'])
                
                if vehicle_exists:
                    cursor.execute("""
                        UPDATE vehicle 
                        SET fleet_id = %s,
                            region_id = %s,
                            vehicle_model_id = %s,
                            licence_plate = %s,
                            end_of_contract_date = %s,
                            start_date = %s
                        WHERE vin = %s
                    """, (
                        fleet_id,
                        region_id,
                        vehicle_model_id,
                        vehicle_data['Licence plate'],
                        end_of_contract,
                        start_date,
                        vin
                    ))
                    logging.info(f"Updated vehicle with VIN: {vin}")
                else:
                    vehicle_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO vehicle (
                            id, vin, fleet_id, region_id, vehicle_model_id,
                            licence_plate, end_of_contract_date, start_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        vehicle_id, vin, fleet_id, region_id, vehicle_model_id,
                        vehicle_data['Licence plate'],
                        end_of_contract,
                        start_date
                    ))
                    logging.info(f"Inserted new vehicle with VIN: {vin}")
                
                await asyncio.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                logging.error(f"Error processing VIN {vin}: {str(e)}")
                continue
        
        conn.commit()
    
    return []

async def read_fleet_info(ownership_filter: str = None) -> pd.DataFrame:
    """
    Lit le fichier fleet_info.csv et retourne un DataFrame
    
    Args:
        ownership_filter (str, optional): Filtre pour ne garder qu'un certain type d'ownership
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, '..', 'data', 'fleet_info.csv')
        
        df = pd.read_csv(file_path)
        
        df.columns = [col if col != " " else "brand" for col in df.columns]
        df = df[df['brand'].str.lower() == 'tesla']
        df['Country'] = df['Country'].replace('NL', 'netherlands')
        
        if ownership_filter:
            df['Ownership_lower'] = df['Ownership '].str.lower()
            df = df[df['Ownership_lower'] == ownership_filter.lower()]
            df = df.drop('Ownership_lower', axis=1) 
            
            logging.info(f"Filtré pour Ownership = {ownership_filter}")        
        return df
        
    except FileNotFoundError:
        logging.error(f"Le fichier fleet_info.csv n'a pas été trouvé dans {file_path}")
        raise
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier CSV: {str(e)}")
        raise

async def test_vin_matching():
    """Test function to compare VINs between fleet_info and account_vins_mapping"""
    try:
        df = await read_fleet_info()
        excel_vins = set(df['VIN'].unique())
        logging.info(f"Nombre de VINs uniques dans fleet_info: {len(excel_vins)}")
        
        async with aiohttp.ClientSession() as session:
            account_vins_mapping = await get_account_vins_mapping(session)
            all_mapped_vins = set()
            for account_vins in account_vins_mapping.values():
                all_mapped_vins.update(account_vins)
            
            logging.info(f"Nombre de VINs dans account_vins_mapping: {len(all_mapped_vins)}")
            
            matching_vins = excel_vins.intersection(all_mapped_vins)
            logging.info(f"Nombre de VINs qui matchent: {len(matching_vins)}")
            
            missing_vins = excel_vins - all_mapped_vins
            logging.info(f"VINs dans fleet_info mais pas dans mapping: {len(missing_vins)}")
            
            extra_vins = all_mapped_vins - excel_vins
            logging.info(f"VINs dans mapping mais pas dans fleet_info: {len(extra_vins)}")
            
    except Exception as e:
        logging.error(f"Erreur dans test_vin_matching: {str(e)}")

async def main():
    try:
        ownership_filter = "AYVENS"
        df = await read_fleet_info(ownership_filter=ownership_filter)
        logging.info(f"Nombre total de véhicules dans fleet_info: {len(df)}")
        
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

if __name__ == "__main__":
    asyncio.run(main())
