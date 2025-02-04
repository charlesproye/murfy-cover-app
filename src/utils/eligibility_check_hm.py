import asyncio
import pandas as pd
import os
import aiohttp
import logging
import json
from dotenv import load_dotenv
import tkinter as tk
from tkinter import filedialog

load_dotenv()
logging.basicConfig(level=logging.INFO)
RATE_LIMIT_DELAY = 0.5 


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
                    logging.info("Successfully retrieved access token.")
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


async def get_data(session, vin, brand):

    # Use the token in the authorization header
    url = 'https://api.high-mobility.com/v1/eligibility'
    token = await get_oauth2_token(session)
    headers = {
        "Content-Type": "application/json",
        "Authorization": token
    }

    body = {
        "vin": vin,
        "brand": brand
    }

    async with session.post(url, headers=headers, json=body) as response:
        try:
            response.raise_for_status()  
            json_response = await response.json() 
            eligibility = json_response.get('eligible')
            logging.info(f"Vin : {vin} Activation: {eligibility}")
            return eligibility 
        
        except Exception as e:
            # Handle other unexpected exceptions
            logging.error(f"Vin: {vin} - Request failed with error: {str(e)}")
            return False
        
  
async def process_vins_and_brands(df_in):
    async with aiohttp.ClientSession() as session:
        activation_results = []

        # Iterate over each row in df_in
        for _, row in df_in.iterrows():
            vin = row['Chassisnummer']
            brand = row['Merk'].lower()
            eligible = await get_data(session, vin, brand)
            activation_results.append(eligible)
            await asyncio.sleep(RATE_LIMIT_DELAY)

        # Add Activation column to the original DataFrame
        df_in['Activation'] = activation_results
        return df_in

async def main():
    df_in = select_file()  # Get the input DataFrame
    df_out = await process_vins_and_brands(df_in)
    df_out.to_csv('src/utils/eligibility_check.csv', index=False)

asyncio.run(main())
