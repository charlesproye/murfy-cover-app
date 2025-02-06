import asyncio
import pandas as pd
import os
import aiohttp
import logging
from dotenv import load_dotenv
import tkinter as tk
from tkinter import filedialog
import time

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Global variable to store the token
cached_token = None

supported_brands = [
    'alfa-romeo',
    'bmw',
    'citroen',
    'citroën',
    'ds',
    'ford',
    'fiat',
    'lexus',
    'mercedes',
    'mercedes-benz',
    'mini',
    'opel',
    'peugeot',
    'renault',  
    'toyota'
]

def select_file():
    root = tk.Tk()
    root.withdraw() 
    file_path = filedialog.askopenfilename(
        title="Select a CSV file",
        filetypes=(("CSV files", "*.csv"), ("All files", "*.*"))
    )
    
    if file_path and file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        return df
    else:
        print("Invalid file. Please make sure the file is a CSV file.")
        return None

async def get_oauth2_token(session: aiohttp.ClientSession) -> str:
    url = "https://api.high-mobility.com/v1/access_tokens"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("CLIENT_ID_HM"),
        "client_secret": os.getenv("CLIENT_SECRET_HM"),
        "scope": "fleet:clearance vehicle:eligibility-check vehicle:data"
    }

    try:
        async with session.post(url, headers=headers, data=payload) as response:
            if response.status == 200:
                response_data = await response.json()
                access_token = response_data.get("access_token")
                if access_token:
                    return f"Bearer {access_token}"
                else:
                    logging.error("Access token not found in response.")
                    return None
            else:
                logging.error(f"Failed to retrieve token. Status: {response.status}, Response: {await response.text()}")
                return None
    except aiohttp.ClientError as e:
        logging.error(f"HTTP Client error: {e}")
        return None

async def get_data(session, vin, brand):
    global cached_token

    url = 'https://api.high-mobility.com/v1/eligibility'
    
    # Check if we have a valid token, if not, fetch a new one
    if not cached_token:
        cached_token = await get_oauth2_token(session)

    headers = {
        "Content-Type": "application/json",
        "Authorization": cached_token
    }

    body = {
        "vin": vin,
        "brand": brand
    }

    async with session.post(url, headers=headers, json=body) as response:
        if response.status == 401:
            # Check if the error is due to an expired token
            response_data = await response.json()
            errors = response_data.get("errors", [])
            if any(error.get("detail") == "Expired token" for error in errors):
                logging.info("Token expired, fetching a new one.")
                cached_token = await get_oauth2_token(session)
                headers["Authorization"] = cached_token
                async with session.post(url, headers=headers, json=body) as retry_response:
                    return await retry_response.json()
            else:
                logging.error(f"Authorization failed: {response_data}")
                return False
        else:
            return await response.json()

async def process_vins_and_brands(df_in):
    async with aiohttp.ClientSession() as session:
        activation_results = []
        start_time = time.time()
        total_vins = len(df_in)
        
        for index, row in df_in.iterrows():
            vin = row['Chassisnummer']
            brand = row['Merk'].lower()
            if brand not in supported_brands:
                eligibility = None
                activation_results.append(eligibility)
                continue
                
            eligibility_data = await get_data(session, vin, brand)
            eligibility = eligibility_data.get('eligible', False)
            
            elapsed_time = time.time() - start_time
            progress = f"[{index + 1}/{total_vins}]"
            
            logging.info(
                f"{progress} Processed VIN: {vin:<17} | "
                f"Brand: {brand:<15} | "
                f"Eligible: {str(eligibility):<5} | "
                f"Time: {elapsed_time:.2f}s"
            )
            
            activation_results.append(eligibility)
            
        df_in['Activation'] = activation_results
        return df_in

async def main():
    df_in = select_file()  # Get the input DataFrame
    if df_in is not None:
        df_out = await process_vins_and_brands(df_in)
        df_out.to_csv('src/utils/eligibility_check.csv', index=False)

asyncio.run(main())
