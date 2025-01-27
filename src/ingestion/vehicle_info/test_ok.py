import asyncio
import aiohttp
import json
import os
import logging
from typing import List, Dict
from core.sql_utils import con
import uuid
import pandas as pd
import time
import re

from core.sql_utils import get_connection
from fleet_info import read_fleet_info as fleet_info
from dotenv import load_dotenv
from datetime import datetime, timedelta

async def get_start_date(session: aiohttp.ClientSession, access_token: str, vin: str) -> str:
    """Récupère la warranty expiration date et le coverageAgeInYears et fait leur différence pour calculer la start_date"""
    url = f"https://fleet-api.prd.eu.vn.cloud.tesla.com/api/1/dx/warranty/details?vin={vin}"
    headers = {'Authorization': f'Bearer {access_token}'}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                activeWarranty = data.get("activeWarranty")
                

                if activeWarranty:  
                    warranty = activeWarranty[0]  

                    expirationDate = warranty.get("expirationDate")

                    coverageAgeInYears = warranty.get("coverageAgeInYears")
                    
                    # Convert expirationDate to a datetime object
                    print("if expirationDate is not None and coverageAgeInYears is not None")
                    expi= expirationDate.split('T')[0]
                    print(expi)
                    expiration_date_obj = datetime.strptime(expi, '%Y-%m-%d')
                    print('ok')
                    # Subtract coverageAgeInYears from the expiration date
                    start_date_obj = expiration_date_obj - timedelta(days=coverageAgeInYears * 365.25) # Approximate years with 365.25 days per year
                    print(start_date_obj)
                    # Convert the start date back to ISO format string
                    start_date_str = start_date_obj.strftime('%Y-%m-%d')

                    print(start_date_str)
                    
                    return start_date_str
                else:
                    return None
            else:
                return None
    except Exception as e:
        return f"Error: {str(e)}"
        return None
    

async def main():
    try:
        token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InFEc3NoM2FTV0cyT05YTTdLMzFWV0VVRW5BNCJ9.eyJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJzdWIiOiI4ZDc0ZjFlMS05NDM3LTQwZmItYjYwYi04ODFjZGVlMmQ2MmYiLCJpc3MiOiJodHRwczovL2F1dGgudGVzbGEuY29tL29hdXRoMi92My9udHMiLCJhenAiOiI4ODMyMjc3YWU0Y2MtNDQ2MS04Mzk2LTEyNzMxMDEyOWRjNiIsImF1ZCI6WyJodHRwczovL2F1dGgudGVzbGEuY29tL29hdXRoMi92My9jbGllbnRpbmZvIiwiaHR0cHM6Ly9mbGVldC1hcGkucHJkLmV1LnZuLmNsb3VkLnRlc2xhLmNvbSJdLCJleHAiOjE3MzgwMjYwMjgsImlhdCI6MTczNzk5NzIyOCwiYWNjb3VudF90eXBlIjoiYnVzaW5lc3MiLCJvcGVuX3NvdXJjZSI6bnVsbCwiYWN0Ijp7InN1YiI6IjkxZjZiZTJlLTFkYWQtMTFlOS05MDc3LTAwNTA1NjlhOTJlOSIsInNjcCI6WyJ1c2VyX2RhdGEiLCJ2ZWhpY2xlX2RldmljZV9kYXRhIiwidmVoaWNsZV9jbWRzIiwidmVoaWNsZV9jaGFyZ2luZ19jbWRzIiwiZW5lcmd5X2NtZHMiLCJlbmVyZ3lfZGV2aWNlX2RhdGEiXX19.SHoYnBaS5_eohr3YHpoQAtJBWWUM0sal2fwoU9uf7fkWYTTMehLIXdN_goQ5DIxitS_qSVX8sW91m0xTf_0nuIViFCFarmTmclmV7elqtv9AfueRUjUh-aKdBvEtb0H_9ggGP0_PlZBAZe189fpRag4HtImddpF6FabOl3hO2slnRMUxtt05iqEyzZFeAsW7Iqxd-GSacLM5BlytRAtLoJ-zDH_s5Rhh2RwkFTUtRdnvzLzI7DdxN4MHkmD2JMRZ_zJVRHZhG7nKflVh1SrLA6pHtHRFQtEPfkOWUINy0vDOVYCzRiqIuraeJA6WMehLsETQ69aMTPatLtXtShBk6w'
        async with aiohttp.ClientSession() as session:
            await get_start_date(session, token, "LRW3E7EC4MC342854")
                    
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")

if __name__ == "__main__":

    asyncio.run(main())
