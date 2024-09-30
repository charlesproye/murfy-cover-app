import requests
import datetime
import time
import logging
import os
from dotenv import load_dotenv
from data_processor import extract_relevant_data
from s3_handler import save_data_to_s3
from utils import get_token, update_tokens, wake_up_vehicle, refresh_token_and_retry_request

def fetch_vehicle_data(vehicle_id, access_token, refresh_token, access_token_key, refresh_token_key):
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/vehicle_data?endpoints=charge_state%3Bclimate_state%3Bclosures_state%3Bdrive_state%3Bvehicle_state%3Bvehicle_config"
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 401:
        logging.warning("Access token expired. Refreshing token.")
        tokens = refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key)
        if tokens:
            return fetch_vehicle_data(vehicle_id, tokens['access_token'], tokens['refresh_token'], access_token_key, refresh_token_key)
        else:
            logging.error("Failed to refresh token.")
            return False
    elif response.status_code == 429:
        logging.warning(f"Rate limit exceeded for vehicle {vehicle_id}.")
        return 'rate_limit'
    elif response.status_code == 408:
        logging.warning(f"Vehicle is asleep. Waking up the vehicle : {vehicle_id}")
        response_wake_up = wake_up_vehicle(access_token, vehicle_id)
        time.sleep(8)
        if response_wake_up == 'rate_limit':
            logging.warning(f"Rate limit hit during wake up for vehicle {vehicle_id}.")
            return 'rate_limit'
        if response_wake_up:
            logging.info(f"Vehicle waked up successfully. : {vehicle_id}")
            return 'job_retry'
        else:
            return False
    elif response.status_code != 200:
        logging.error(f"Failed to fetch vehicle data: {response.status_code}")
        return False
    
    return response.json()

def fetch_all_vehicle_ids(access_token_key, refresh_token_key):
    load_dotenv(override=True)
    access_token = get_token(access_token_key)
    refresh_token = get_token(refresh_token_key)
    url = "https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles"
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        vehicles = response.json()['response']
        return [vehicle['vin'] for vehicle in vehicles]
    elif response.status_code == 401 or response.status_code == 421:  # Token expired
        logging.warning("Access token expired. Refreshing token.")
        tokens = refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key)
        if tokens:
            time.sleep(2)
            return fetch_all_vehicle_ids(access_token_key, refresh_token_key)
        else:
            logging.error("Failed to refresh token and fetch vehicle IDs.")
            return []
    else:
        logging.error(f"Failed to fetch vehicles: {response.status_code}")
        return []

def job(vehicle_id, access_token_key, refresh_token_key):
    access_token = get_token(access_token_key)
    refresh_token = get_token(refresh_token_key)

    if not access_token or not refresh_token:
        logging.error(f"Failed to retrieve tokens for vehicle {vehicle_id}")
        return

    try:
        vehicle_data = fetch_vehicle_data(vehicle_id, access_token, refresh_token, access_token_key, refresh_token_key)
        if vehicle_data == 'rate_limit':
            logging.warning(f"Rate limit hit for vehicle {vehicle_id}. Skipping data extraction.")
        elif vehicle_data == 'job_retry':
            logging.info(f"Retrying job for {vehicle_id} after a brief pause.")
            time.sleep(10)
            job(vehicle_id, access_token_key, refresh_token_key)
        elif vehicle_data:
            relevant_data = extract_relevant_data(vehicle_data, vehicle_id)
            if relevant_data:
                save_data_to_s3(relevant_data, vehicle_id)
        else:
            logging.error(f"Failed to process data for {vehicle_id}")
    except Exception as e:
        logging.error(f"An error occurred in job execution for vehicle {vehicle_id}: {e}")
