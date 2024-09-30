import requests
import time
import logging
import os
import redis

# Configuration Redis
redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

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

def wake_up_vehicle(access_token, vehicle_id):
    max_attempts = 3
    attempt = 0
    delay = 5  # Délai initial entre les tentatives en secondes

    while attempt < max_attempts:
        current_time = time.time()
        if hasattr(wake_up_vehicle, 'last_wake_up_time') and vehicle_id in wake_up_vehicle.last_wake_up_time:
            if (current_time - wake_up_vehicle.last_wake_up_time[vehicle_id]) < 60:
                logging.info(f"Vehicle {vehicle_id} was recently woken up. Skipping wake up.")
                return True

        url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/wake_up"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        try:
            response = requests.post(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logging.info(f"Vehicle {vehicle_id} woken up successfully.")
                if not hasattr(wake_up_vehicle, 'last_wake_up_time'):
                    wake_up_vehicle.last_wake_up_time = {}
                wake_up_vehicle.last_wake_up_time[vehicle_id] = current_time
                return True
            elif response.status_code == 408:
                logging.warning(f"Timeout while waking up vehicle {vehicle_id}. Attempt {attempt + 1}/{max_attempts}")
            elif response.status_code == 429:
                logging.warning(f"Rate limit exceeded while trying to wake up vehicle {vehicle_id}. Attempt {attempt + 1}/{max_attempts}")
                return 'rate_limit'
            else:
                logging.error(f"Failed to wake up vehicle {vehicle_id}: HTTP {response.status_code}. Attempt {attempt + 1}/{max_attempts}")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error while trying to wake up vehicle {vehicle_id}: {e}. Attempt {attempt + 1}/{max_attempts}")
        
        attempt += 1
        if attempt < max_attempts:
            time.sleep(delay)
            delay *= 2  # Augmentation exponentielle du délai

    logging.error(f"Failed to wake up vehicle {vehicle_id} after {max_attempts} attempts.")
    return False
