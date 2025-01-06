
import asyncio
import aiohttp
import json
import os
import logging
from typing import List, Dict
from core.sql_utils import get_connection
import uuid
import pandas as pd
import time
import re
from ingestion.fleet_info.other import *
from ingestion.fleet_info.tesla import *

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


async def main():
    try:
        #on peut mettre ce qu'on veut ici
        ownership_filter = "Ayvens" 
        df = await read_fleet_info(ownership_filter)
        logging.info(f"Nombre total de véhicules dans fleet_info: {len(df)}")
        
        await process_vehicles(df)
        await list_used_models()
        await cleanup_unused_models()
        # metadata = await get_existing_model_metadata()
        
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
