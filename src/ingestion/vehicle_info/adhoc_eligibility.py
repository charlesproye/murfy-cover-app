import asyncio
import logging
import pandas as pd
import signal
import sys
from typing import Optional, List
from functools import partial
from dataclasses import dataclass
import aiohttp

from ingestion.vehicle_info.config.settings import LOGGING_CONFIG
from ingestion.vehicle_info.config.credentials import *
from ingestion.vehicle_info.api.bmw_client import BMWApi
from ingestion.vehicle_info.api.hm_client import HMApi
from ingestion.vehicle_info.api.stellantis_client import StellantisApi
from ingestion.vehicle_info.api.tesla_client import TeslaApi
from ingestion.vehicle_info.services.activation_service import VehicleActivationService
from ingestion.vehicle_info.services.vehicle_processor import VehicleProcessor
from ingestion.vehicle_info.fleet_info import read_fleet_info as fleet_info
from ingestion.vehicle_info.utils.google_sheets_utils import get_google_client

logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)
brand_mapping = {
    'citroën': 'citroen',
    'dsautomobiles': 'ds',
    'volvo': 'volvo-cars',
    'mercedes': 'mercedes-benz'
}
async def check_eligibility():
    # Lire le fichier CSV
    df = pd.read_csv('src/ingestion/vehicle_info/vin_autoviza.csv')
    
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
            vin = row['codif_vin']
            brand = row['marque'].strip().lower()
            brand = brand_mapping.get(brand, brand)
            if brand not in ['alfaromeo', 'bmw', 'citroen', 'dsautomobiles', 'ford', 'fiat', 'mercedes', 'mercedesbenzfleets', 'mini', 'opel', 'volvocars','renault','peugeot']:
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
    output_file = 'src/ingestion/vehicle_info/vin_autoviza_updated.csv'
    df.to_csv(output_file, index=False)
    logger.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    asyncio.run(check_eligibility())


