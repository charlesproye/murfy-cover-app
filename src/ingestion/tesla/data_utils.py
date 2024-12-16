import logging
import os
import redis
import random
import asyncio
import aiohttp
import json

# Configuration Redis
redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
redis_password = os.environ.get('REDIS_PASSWORD')
redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

MAX_WAKE_UP_ATTEMPTS = 5
WAKE_UP_WAIT_TIME = 30  # seconds

# Ajout d'un verrou global pour le refresh des tokens
token_refresh_lock = asyncio.Lock()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

async def fetch_slack_messages(channel_id, slack_token):
    url = f"https://slack.com/api/conversations.history?channel={channel_id}"
    headers = {
        'Authorization': f'Bearer {slack_token}',
        'Content-Type': 'application/json'
    }
    
    async with aiohttp.ClientSession() as session:
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

async def update_token_from_slack(access_token_key):
    """Mise à jour du token depuis Slack de manière sécurisée"""
    async with token_refresh_lock:  # Utilisation du même verrou
        try:
            logging.info("Starting Slack token update process...")
            await asyncio.sleep(1)  # Délai initial
            
            slack_token = os.getenv('SLACK_TOKEN')
            channel_id = 'C0816LXFCNL'
            
            if not slack_token:
                logging.error("Slack bot token is not set.")
                return None
            
            messages = await fetch_slack_messages(channel_id, slack_token)
            
            for i in range(6):
                try:
                    message_text = messages[i]['blocks'][0]['elements'][0]['elements'][3]['text']
                    response_key = messages[i]['blocks'][0]['elements'][0]['elements'][0]['text'].split(':')[0]
                    response_data = json.loads(message_text)
                    
                    if response_key == access_token_key:
                        logging.info(f"Found token for {access_token_key} in Slack")
                        await update_tokens_code(response_data['access_token'], access_token_key)
                        await asyncio.sleep(2)  # Attendre avant de retourner
                        return {"key": response_key, "token": response_data['access_token']}
                except (KeyError, IndexError, json.JSONDecodeError) as e:
                    continue
            
            logging.warning(f"No token found for {access_token_key} in Slack")
            await asyncio.sleep(2)
            return None
            
        except Exception as e:
            logging.error(f"Error updating token from Slack: {str(e)}")
            await asyncio.sleep(2)
            return None

async def refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key):
    """Refresh token de manière sécurisée et séquentielle"""
    async with token_refresh_lock:  # Utilisation du verrou
        try:
            logging.info("Starting token refresh process...")
            await asyncio.sleep(1)  # Petit délai pour s'assurer que tout est calme
            
            url = "https://auth.tesla.com/oauth2/v3/token"
            headers = {'Content-Type': 'application/json'}
            payload = {
                'grant_type': 'refresh_token',
                'client_id': '8832277ae4cc-4461-8396-127310129dc6',
                'refresh_token': refresh_token
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        new_tokens = await response.json()
                        access_token = new_tokens['access_token']
                        refresh_token = new_tokens['refresh_token']
                        
                        # Mise à jour des tokens dans Redis
                        await update_tokens(access_token, refresh_token, access_token_key, refresh_token_key)
                        logging.info("Tokens refreshed and updated successfully.")
                        
                        # Attendre un peu avant de reprendre
                        await asyncio.sleep(2)
                        return {'access_token': access_token, 'refresh_token': refresh_token}
                    else:
                        error_body = await response.text()
                        logging.error(f"Failed to refresh token: {response.status}, {error_body}")
                        if response.status == 401:
                            logging.error("Authentication failed. The refresh token may be invalid or expired.")
                        await asyncio.sleep(2)  # Attendre avant de retourner l'erreur
                        return None
        except Exception as e:
            logging.error(f"Error during token refresh: {str(e)}")
            await asyncio.sleep(2)
            return None

async def update_tokens_code(access_token, access_token_key):
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: redis_client.set(access_token_key, access_token)
        )
        logging.info("Tokens updated successfully in Redis.")
    except redis.RedisError as e:
        logging.error(f"Failed to update tokens in Redis: {str(e)}")

async def update_tokens(access_token, refresh_token, access_token_key, refresh_token_key):
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: redis_client.set(access_token_key, access_token)
        )
        await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: redis_client.set(refresh_token_key, refresh_token)
        )
        logging.info("Tokens updated successfully in Redis.")
    except redis.RedisError as e:
        logging.error(f"Failed to update tokens in Redis: {str(e)}")

async def get_token(token_key):
    try:
        # Vérifier que token_key n'est pas None
        if token_key is None:
            logging.error("Token key is None")
            return None
        token = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: redis_client.get(token_key)
        )
                
        if token is None:
            logging.warning(f"Token {token_key} not found in Redis")
            return None
            
        return token
        
    except redis.RedisError as e:
        logging.error(f"Failed to retrieve token from Redis: {str(e)}")
        return None
    
async def get_token_from_auth_code(code):
    """Get access token using authorization code."""
    url = "https://auth.tesla.com/oauth2/v3/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': '8832277ae4cc-4461-8396-127310129dc6',
        'client_secret': 'ta-secret.$AXtiMu2kTU%XdTc',
        'audience': 'https://fleet-api.prd.eu.vn.cloud.tesla.com',
        'auth_code': code,
        'scope': 'user_data vehicle_device_data vehicle_cmds vehicle_charging_cmds'
    }
        # Important : utiliser aiohttp.FormData pour x-www-form-urlencoded
    form_data = aiohttp.FormData()
    for key, value in payload.items():
        form_data.add_field(key, value)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=form_data, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('access_token')
            else:
                logging.error(f"Failed to get token from auth code: {await response.text()}")
                return None

def exponential_backoff(attempt, base_delay=5, max_delay=300):
    delay = min(base_delay * (2 ** attempt), max_delay)
    return delay + random.uniform(0, delay * 0.1)  # Ajoute un peu de "jitter"  

async def wake_up_vehicle(access_token, vehicle_id):
    attempts = 0
    while attempts < MAX_WAKE_UP_ATTEMPTS:
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/wake_up"
        headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        return True
                    elif response.status == 408:
                        logging.warning(f"Timeout while waking up vehicle {vehicle_id}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
                    elif response.status == 429:
                        logging.warning(f"Rate^ limit exceeded while trying to wake up vehicle {vehicle_id}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
                        return 'rate_limit'
                    else:
                        logging.error(f"Failed to wake up vehicle {vehicle_id}: HTTP {response.status}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
        
        except aiohttp.ClientError as e:
            logging.error(f"Network error while trying to wake up vehicle {vehicle_id}: {e}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
        
        attempts += 1
        if attempts < MAX_WAKE_UP_ATTEMPTS:
            delay = exponential_backoff(attempts)
            logging.info(f"Waiting {delay:.2f} seconds before next wake-up attempt.")
            await asyncio.sleep(delay)
    logging.error(f"Failed to wake up vehicle {vehicle_id} after {MAX_WAKE_UP_ATTEMPTS} attempts.")
    return False

async def get_vehicle_state(access_token, vehicle_id):
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['response']['state']
                else:
                    logging.error(f"Failed to get vehicle state: HTTP {response.status}")
                    return None
    except aiohttp.ClientError as e:
        logging.error(f"Network error while getting vehicle state: {e}")
        return None
