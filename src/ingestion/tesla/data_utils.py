import requests
import time
import logging
import os
import redis
import random
import asyncio
import aiohttp

# Configuration Redis
redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
redis_password = os.environ.get('REDIS_PASSWORD')
redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

MAX_WAKE_UP_ATTEMPTS = 5
WAKE_UP_WAIT_TIME = 12  # seconds

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

async def refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key):
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
                await update_tokens(access_token, refresh_token, access_token_key, refresh_token_key)
                logging.info("Tokens refreshed successfully.")
                return {'access_token': access_token, 'refresh_token': refresh_token}
            else:
                error_body = await response.text()
                logging.error(f"Failed to refresh token: {response.status}, {error_body}")
                if response.status == 401:
                    logging.error("Authentication failed. The refresh token may be invalid or expired.")
                    # Vous pouvez ajouter ici une logique pour gérer un token invalide,
                    # comme notifier l'utilisateur ou réinitialiser le processus d'authentification
                raise Exception(f"Token refresh failed with status {response.status}")

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
        token = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: redis_client.get(token_key)
        )
        if token is None:
            raise Exception(f"Token {token_key} not found in Redis.")
        return token
    except redis.RedisError as e:
        logging.error(f"Failed to retrieve token from Redis: {str(e)}")
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
                        logging.info(f"Vehicle {vehicle_id} woken up successfully.")
                        return True
                    elif response.status == 408:
                        logging.warning(f"Timeout while waking up vehicle {vehicle_id}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
                    elif response.status == 429:
                        logging.warning(f"Rate limit exceeded while trying to wake up vehicle {vehicle_id}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
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
