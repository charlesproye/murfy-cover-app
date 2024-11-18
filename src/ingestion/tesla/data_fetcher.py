import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta
import time
import logging
import os
import csv
from dotenv import load_dotenv
from data_processor import extract_relevant_data
from s3_handler import save_data_to_s3
from data_utils import get_token, wake_up_vehicle, refresh_token_and_retry_request, get_token_from_auth_code, refresh_token_and_retry_request_code, WAKE_UP_WAIT_TIME, update_token_from_slack
from fleet_manager import VehiclePool
import random

async def handle_wake_up(session, headers, vehicle_id, access_token, vehicle_pool):
    """Gère le réveil d'un véhicule avec le VehiclePool"""
    max_retries = 2
    retry_count = 0
    
    if vehicle_id in vehicle_pool.wake_up_tasks:
        logging.info(f"Wake-up already in progress for vehicle {vehicle_id}")
        try:
            await vehicle_pool.wake_up_tasks[vehicle_id]
            return True
        except Exception:
            return False
    
    wake_up_url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/wake_up"
    
    while retry_count < max_retries:
        try:
            async with session.post(wake_up_url, headers=headers) as response:
                if response.status == 200:
                    logging.info(f"Wake up command sent successfully to vehicle {vehicle_id}, waiting for vehicle to wake up")
                    # Attendre que le véhicule se réveille
                    await asyncio.sleep(WAKE_UP_WAIT_TIME * (retry_count + 1))
                    
                    # Vérifier si le véhicule est réveillé
                    check_url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/vehicle_data"
                    async with session.get(check_url, headers=headers) as check_response:
                        if check_response.status == 200:
                            logging.info(f"Vehicle {vehicle_id} is now awake")
                            return True
                        else:
                            logging.info(f"Vehicle {vehicle_id} is still waking up (status: {check_response.status})")
                    
                elif response.status == 429:
                    return 'rate_limit'
            
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(WAKE_UP_WAIT_TIME)
        except Exception as e:
            logging.error(f"Error during wake up for vehicle {vehicle_id}: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(WAKE_UP_WAIT_TIME)
    
    logging.info(f"Vehicle {vehicle_id} wake up command sent, vehicle will be checked again in 30 minutes")
    return False

async def fetch_vehicle_data_with_retry(session, url, headers, max_retries=3, base_delay=1):
    """Fonction utilitaire pour les appels API avec retry"""
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                logging.debug(f"API Response status: {response.status}, URL: {url}")
                
                if response.status == 408:
                    return response.status, 'sleeping'
                elif response.status == 429:
                    return response.status, 'rate_limit'
                elif response.status == 405:
                    logging.error(f"Method not allowed (405) - Vehicle might be offline or unavailable")
                    return response.status, 'offline'
                elif response.status == 401 or response.status == 421:
                    return response.status, await response.json()
                elif response.status == 200:
                    return response.status, await response.json()
                else:
                    response_text = await response.text()
                    logging.error(f"Failed request with status {response.status}. Response: {response_text}")
                    return response.status, None
                    
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
            logging.warning(f"Connection error, retrying in {delay:.1f}s: {str(e)}")
            await asyncio.sleep(delay)
    return None, None

async def fetch_vehicle_data(vehicle_id, access_token, refresh_token, access_token_key, refresh_token_key, vehicle_pool, auth_code=None):
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles/{vehicle_id}/vehicle_data"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        try:
            status, data = await fetch_vehicle_data_with_retry(session, url, headers)
            
            if status == 408:
                logging.info(f"Vehicle {vehicle_id} is sleeping, attempting wake up")
                wake_up_result = await handle_wake_up(session, headers, vehicle_id, access_token, vehicle_pool)
                if wake_up_result == True:
                    # Réessayer de récupérer les données après le réveil
                    status, data = await fetch_vehicle_data_with_retry(session, url, headers)
                    if status == 200:
                        return data
                return 'sleeping'
            elif status == 405:
                logging.info(f"Vehicle {vehicle_id} is offline or unavailable")
                return 'offline'
            elif status == 401 or status == 421:
                logging.warning("Access token expired. Refreshing token.")
                if refresh_token:
                    logging.info("Using refresh token to get new access token")
                    tokens = await refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key)
                    if tokens:
                        return await fetch_vehicle_data(vehicle_id, tokens['access_token'], tokens['refresh_token'], access_token_key, refresh_token_key, vehicle_pool)
                elif auth_code:
                    logging.info("Using auth code to get new access token")
                    tokens = await update_token_from_slack(access_token_key)
                    if tokens:
                        return await fetch_vehicle_data(vehicle_id, tokens['token'], None, access_token_key, refresh_token_key, vehicle_pool, auth_code)
                logging.error("Failed to refresh token.")
                return False
            elif status == 429:
                logging.warning(f"Rate limit exceeded for vehicle {vehicle_id}.")
                return 'rate_limit'
            elif status == 200 and data:
                return data
            else:
                logging.error(f"Failed to fetch vehicle data: Status {status}")
                return False
            
        except Exception as e:
            logging.error(f"Error fetching data for vehicle {vehicle_id}: {str(e)}")
            return False

async def fetch_all_vehicle_ids(access_token_key, refresh_token_key, csv_path: str = None, auth_code:str = None):
    load_dotenv(override=True)
    access_token = await get_token(access_token_key)
    refresh_token = await get_token(refresh_token_key) if refresh_token_key else None
    base_url = "https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/vehicles"
    headers = {'Authorization': f'Bearer {access_token}'}

    authorized_vins = set()
    if csv_path:
        try:
            with open(csv_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                next(csv_reader)  # Skip header row if present
                authorized_vins = set(
                    row[0].strip() 
                    for row in csv_reader 
                    if row and row[2] == "TESLA"
                )
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
    
    async with aiohttp.ClientSession() as session:
        all_vehicles = []
        page = 1
        
        while True:
            url = f"{base_url}?page={page}"
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    vehicles = data['response']
                    all_vehicles.extend(vehicles)
                    
                    # Check pagination info
                    if 'pagination' in data and data['pagination']['next']:
                        page = data['pagination']['next']
                    else:
                        break
                        
                elif response.status == 401 or response.status == 421:  # Token expired
                    logging.warning("Access token expired. Refreshing token.")
                    if refresh_token:
                        tokens = await refresh_token_and_retry_request(access_token, refresh_token, access_token_key, refresh_token_key)
                    else:
                        tokens = await update_token_from_slack(access_token_key)
                    if tokens:
                        await asyncio.sleep(2)
                        return await fetch_all_vehicle_ids(access_token_key, refresh_token_key, csv_path, auth_code)
                    else:
                        logging.error("Failed to refresh token and fetch vehicle IDs.")
                        return []
                else:
                    logging.error(f"Failed to fetch vehicles: {response.status}")
                    return []

        account_vins = [vehicle['vin'] for vehicle in all_vehicles]
        
        if authorized_vins:
            authorized_account_vins = [vin for vin in account_vins if vin in authorized_vins]
        else:
            authorized_account_vins = account_vins
        
        return authorized_account_vins

async def job(vehicle_id, access_token_key, refresh_token_key, vehicle_pool, auth_code=None):
    """
    Process a single vehicle
    """
    try:
        access_token = await get_token(access_token_key)
        refresh_token = await get_token(refresh_token_key) if refresh_token_key else None

        if not access_token:
            logging.error(f"Failed to retrieve tokens for vehicle {vehicle_id}")
            return

        try:
            # Vérifier si le véhicule est prêt à être traité
            current_time = datetime.now()
            if not vehicle_pool._is_vehicle_ready(vehicle_id, current_time):
                return

            vehicle_data = await fetch_vehicle_data(
                vehicle_id, 
                access_token, 
                refresh_token, 
                access_token_key, 
                refresh_token_key,
                vehicle_pool,
                auth_code
            )
            
            if vehicle_data == 'rate_limit':
                logging.warning(f"Rate limit hit for vehicle {vehicle_id}")
                vehicle_pool.update_vehicle_status(vehicle_id, False)
            elif vehicle_data == 'sleeping':
                vehicle_pool.update_vehicle_status(vehicle_id, True, is_sleeping=True)
                next_check = datetime.now() + timedelta(minutes=10)
                logging.info(f"Vehicle {vehicle_id} is sleeping, next check at {next_check.strftime('%H:%M:%S')}")
            elif vehicle_data:
                relevant_data = extract_relevant_data(vehicle_data, vehicle_id)
                if relevant_data:
                    await save_data_to_s3(relevant_data, vehicle_id)
                    vehicle_pool.update_vehicle_status(vehicle_id, True)
                else:
                    vehicle_pool.update_vehicle_status(vehicle_id, False)
            else:
                logging.error(f"Failed to process data for {vehicle_id}")
                vehicle_pool.update_vehicle_status(vehicle_id, False)
        except Exception as e:
            logging.error(f"An error occurred in job execution for vehicle {vehicle_id}: {str(e)}")
            if isinstance(vehicle_pool, VehiclePool):
                vehicle_pool.update_vehicle_status(vehicle_id, False)
    finally:
        # Toujours libérer le véhicule à la fin du traitement
        vehicle_pool.release_vehicle(vehicle_id)

