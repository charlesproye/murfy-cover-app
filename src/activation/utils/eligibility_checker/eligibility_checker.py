import asyncio
import logging
import pandas as pd
import signal
import sys
from typing import Optional, List
from functools import partial
from dataclasses import dataclass
import aiohttp
import tkinter as tk
from tkinter import filedialog

from ingestion.vehicle_info.config.settings import LOGGING_CONFIG
from ingestion.vehicle_info.config.credentials import *
from ingestion.vehicle_info.api.hm_client import HMApi

logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)
brand_mapping = {
    'citroën': 'citroen',
    'volvo': 'volvo-cars',
    'mercedes': 'mercedes-benz',
    'b.m.w.': 'bmw',
}
async def check_eligibility():
    # Create a root window and hide it
    root = tk.Tk()
    root.withdraw()
    
    # Open file dialog to select CSV file
    file_path = filedialog.askopenfilename(
        title="Select CSV file",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )
    
    if not file_path:
        logger.error("No file selected. Exiting...")
        return
    
    # Lire le fichier CSV
    df = pd.read_csv(file_path)
    
    # Initialiser le client HM
    hm_client = HMApi(
        base_url=HM_BASE_URL,
        client_id=HM_CLIENT_ID,
        client_secret=HM_CLIENT_SECRET
    )
    
    # Créer une session aiohttp
    async with aiohttp.ClientSession() as session:
        # Pour chaque ligne du DataFrame
        for index, row in df.iterrows():
            vin = row['vin']
            brand = row['brand'].strip().lower()
            brand = brand_mapping.get(brand, brand)
            if brand == 'tesla':
                df.at[index, 'Eligibility'] = 'TRUE'
                continue
            elif brand not in ['bmw','citroen','ds','volvo-cars','fiat','ford','mini','mercedes-benz','opel','peugeot','renault']:
                df.at[index, 'Comment'] = 'Brand not supported'
                continue
            try:
                is_eligible = await hm_client.get_eligibility(vin, brand, session)
                print(vin, brand, is_eligible)
                # Mettre à jour la colonne Eligibility
                df.at[index, 'Eligibility'] = str(is_eligible).upper()
                
                # Afficher la progression
                if index % 100 == 0:
                    logger.info(f"Processed {index} VINs")
                    
            except Exception as e:
                logger.error(f"Error processing VIN {vin}: {str(e)}")
                df.at[index, 'Eligibility'] = 'ERROR'
                df.at[index, 'Comment'] = str(e)
    
    # Sauvegarder le résultat
    output_file = 'src/ingestion/vehicle_info/vin_updated.csv'
    df.to_csv(output_file, index=False)
    logger.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    asyncio.run(check_eligibility())

