from core.sql_utils import get_connection
import aiohttp
import asyncio
import logging
import re
from ingestion.vehicle_info.config.mappings import TESLA_MODEL_MAPPING
from datetime import datetime
from dateutil.relativedelta import relativedelta

class TeslaParticulierApi:
    TESLA_PATTERNS = {
            'model 3': {
                'patterns': [
                    (r'.*standard range.*plus.*rear.?wheel.*|.*standard range.*plus.*rwd.*|.*rear.?wheel drive.*', 'rwd'),
                    (r'.*performance.*dual motor.*|.*performance.*', 'performance'),
                    (r'.*long range.*all.?wheel drive.*', 'long range awd'),
                ]
            },
            'model s': {
                'patterns': [
                    (r'.*100d.*', '100d'),
                    (r'.*75d.*', '75d'),
                    (r'.*long range.*plus.*', 'long range plus'),
                    (r'.*long range.*', 'long range'),
                    (r'.*plaid.*', 'plaid'),
                    (r'.*performance.*', 'performance'),
                    (r'.*standard range.*', 'standard range'),
                ]
            },
            'model x': {
                'patterns': [
                    (r'.*long range.*plus.*', 'long range plus'),
                    (r'.*long range.*', 'long range'),
                ]
            },
            'model y': {
                'patterns': [
                    (r'.*long range.*rwd.*', 'long range rwd'),
                    (r'.*long range.*all.?wheel drive.*', 'long range awd'),
                    (r'.*performance.*awd.*', 'performance'),
                    (r'.*rear.?wheel drive.*', 'rwd'),
                ]
            }
        }
    
    def __init__(self, base_url, token_url, client_id):
        self.base_url = base_url
        self.token_url = token_url
        self.client_id = client_id

    async def refresh_tokens(self, vin):
        try:
            with get_connection() as con:
                cursor = con.cursor()
                cursor.execute("""SELECT refresh_token FROM tesla.user_tokens 
                                JOIN tesla.user ON tesla.user.id = tesla.user_tokens.user_id 
                                WHERE vin = %s""", (vin,))
                result = cursor.fetchone()
                print(f"Old refresh token: {result[0]}")
                url = "https://auth.tesla.com/oauth2/v3/token"
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": result[0],
                    "client_id": "8832277ae4cc-4461-8396-127310129dc6"
                }
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    response_data = await response.json()
                    print(f"New refresh token: {response_data['refresh_token']}")
                    if response_data['access_token'] is None or response_data['refresh_token'] is None:
                        print(f"Error refreshing tokens: {response_data}")
                        return None
                    with get_connection() as con:
                        cursor = con.cursor()
                        cursor.execute("""UPDATE tesla.user_tokens 
                                        SET access_token = %s, refresh_token = %s 
                                        WHERE user_id IN (
                                            SELECT id FROM tesla.user WHERE vin = %s
                                        )""", (response_data['access_token'], response_data['refresh_token'], vin))
                        print(f"Refresh Token: {response_data['refresh_token']} fully inserted")
                        con.commit()
                    return response_data['access_token']
        except Exception as e:
            print(f"Error refreshing tokens: {e}")
            return None
                
    async def get_options_particulier(self, vin, access_token):
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/dx/vehicles/options?vin={vin}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    return 'MTU', 'unknown'
                response_data = await response.json()
                data = await response.json()
                model_info = next((item for item in data.get('codes', []) if item['code'].startswith('$MT')), None)
                model_code = vin[3]
                model_name = TESLA_MODEL_MAPPING.get(model_code, 'unknown')
                if not model_info:
                    return 'MTU', 'unknown'

                # Extract version code
                version = model_info['code'][1:]
                if version == 'MTY13':
                    version = 'MTY13C' if vin[10] == 'C' else 'MTY13B'

                # Determine vehicle type
                display_name = model_info['displayName'].lower()
                if model_name not in self.TESLA_PATTERNS:
                    return version, 'unknown'

                # Match vehicle type using patterns
                vehicle_type = next((type_name for pattern, type_name in self.TESLA_PATTERNS[model_name]['patterns']if re.match(pattern, display_name, re.IGNORECASE)),'unknown')
                return version, vehicle_type
            
    async def get_warranty_particulier(self, vin, access_token):
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/dx/warranty/details?vin={vin}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    return None,None,None
                data = await response.json()
                active_warranty = data.get('activeWarranty', [])
                if active_warranty:
                    warranty = active_warranty[1]
                    expiration_date = warranty.get("expirationDate")
                    warranty_date = warranty.get("coverageAgeInYears")
                    warranty_km = int(warranty.get("expirationOdometer"))
                    warranty_km = 240000 if warranty_km == 9999999 else warranty_km
                    
                    expiration_date_obj = datetime.fromisoformat(expiration_date.replace("Z", "+00:00"))
                    start_date_obj = expiration_date_obj - relativedelta(years=int(warranty_date))
                    start_date = start_date_obj.strftime('%Y-%m-%d')
                    return warranty_km,warranty_date,start_date
                return None, None, None
            
            
