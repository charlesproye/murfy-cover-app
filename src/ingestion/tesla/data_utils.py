import requests
import time
import logging
import os
import redis
import random

# Configuration Redis
redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

MAX_WAKE_UP_ATTEMPTS = 5
WAKE_UP_WAIT_TIME = 12  # seconds

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key):
    url = "https://auth.tesla.com/oauth2/v3/token"
    headers = {'Content-Type': 'application/json'}
    payload = {
        'grant_type': 'refresh_token',
        'client_id': '8832277ae4cc-4461-8396-127310129dc6',
        'refresh_token': refresh_token
    }
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        new_tokens = response.json()
        access_token = new_tokens['access_token']
        refresh_token = new_tokens['refresh_token']
        update_tokens(access_token, refresh_token, access_token_key, refresh_token_key)
        logging.info("Tokens refreshed successfully.")
        return {'access_token': access_token, 'refresh_token': refresh_token}
    else:
        logging.error(f"Failed to refresh token: {response.status_code}")
        raise Exception("Token refresh failed")

def update_tokens(access_token, refresh_token, access_token_key, refresh_token_key):
    try:
        redis_client.set(access_token_key, access_token)
        redis_client.set(refresh_token_key, refresh_token)
        logging.info("Tokens updated successfully in Redis.")
    except redis.RedisError as e:
        logging.error(f"Failed to update tokens in Redis: {str(e)}")

def get_token(token_key):
    try:
        token = redis_client.get(token_key)
        if token is None:
            raise Exception(f"Token {token_key} not found in Redis.")
        return token
    except redis.RedisError as e:
        logging.error(f"Failed to retrieve token from Redis: {str(e)}")
        return None

def exponential_backoff(attempt, base_delay=5, max_delay=300):
    delay = min(base_delay * (2 ** attempt), max_delay)
    return delay + random.uniform(0, delay * 0.1)  # Ajoute un peu de "jitter"

def wake_up_vehicle(access_token, vehicle_id):
    attempts = 0
    while attempts < MAX_WAKE_UP_ATTEMPTS:
        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/wake_up"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        try:
            response = requests.post(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logging.info(f"Vehicle {vehicle_id} woken up successfully.")
                return True
            elif response.status_code == 408:
                logging.warning(f"Timeout while waking up vehicle {vehicle_id}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
            elif response.status_code == 429:
                logging.warning(f"Rate limit exceeded while trying to wake up vehicle {vehicle_id}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
                return 'rate_limit'
            else:
                logging.error(f"Failed to wake up vehicle {vehicle_id}: HTTP {response.status_code}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error while trying to wake up vehicle {vehicle_id}: {e}. Attempt {attempts + 1}/{MAX_WAKE_UP_ATTEMPTS}")
        
        attempts += 1
        if attempts < MAX_WAKE_UP_ATTEMPTS:
            delay = exponential_backoff(attempts)
            logging.info(f"Waiting {delay:.2f} seconds before next wake-up attempt.")
            time.sleep(delay)

    logging.error(f"Failed to wake up vehicle {vehicle_id} after {MAX_WAKE_UP_ATTEMPTS} attempts.")
    return False

def get_vehicle_state(access_token, vehicle_id):
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data['response']['state']
        else:
            logging.error(f"Failed to get vehicle state: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error while getting vehicle state: {e}")
        return None
