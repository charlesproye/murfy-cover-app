import logging
import asyncio
import aiohttp
import pandas as pd
import uuid
from typing import Optional, Dict, Any

from ..config.settings import API_TIMEOUT
from ..config.mappings import MAKE_MAPPING, OEM_MAPPING, COUNTRY_MAPPING
from ..utils.validation import validate_vehicle_data
from ..utils.date_utils import convert_date_format
from ..services.activation_service import VehicleActivationService
from core.sql_utils import get_connection

class VehicleProcessor:
    def __init__(self, bmw_api: callable, hm_api: callable, stellantis_api: callable, tesla_api: callable, df: pd.DataFrame):
        self.bmw_api = bmw_api
        self.hm_api = hm_api
        self.stellantis_api = stellantis_api
        self.tesla_api = tesla_api
        self.df = df
    
    async def _get_or_create_oem(self, cursor, oem_raw: str) -> str:
        """Get or create OEM record."""
        oem_lower = OEM_MAPPING.get(oem_raw, oem_raw.lower())
        
        cursor.execute(
            "SELECT id FROM oem WHERE LOWER(oem_name) = %s",(oem_lower,))
        result = cursor.fetchone()
        
        if not result:
            oem_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO oem (id, oem_name) VALUES (%s, %s) RETURNING id",
                (oem_id, oem_lower)
            )
            return cursor.fetchone()[0]
        return result[0]

    async def _get_or_create_make(self, cursor, make_raw: str, oem_id: str) -> str:
        """Get or create Make record."""
        make_lower = MAKE_MAPPING.get(make_raw, make_raw.lower())
        
        cursor.execute(
            "SELECT id FROM make WHERE LOWER(make_name) = %s",
            (make_lower,)
        )
        result = cursor.fetchone()
        
        if not result:
            make_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO make (id, make_name, oem_id) VALUES (%s, %s, %s) RETURNING id",
                (make_id, make_lower, oem_id)
            )
            return cursor.fetchone()[0]
        return result[0]

    async def _get_fleet_id(self, cursor, owner: str) -> Optional[str]:
        """Get Fleet ID by owner name."""
        cursor.execute(
            "SELECT id FROM fleet WHERE LOWER(fleet_name) = LOWER(%s)",
            (owner,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    async def _get_or_create_region(self, cursor, country: str) -> str:
        """Get or create Region record."""
        country = COUNTRY_MAPPING.get(country, country)
        
        cursor.execute(
            "SELECT id FROM region WHERE LOWER(region_name) = LOWER(%s)",
            (country,)
        )
        result = cursor.fetchone()
        
        if not result:
            region_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO region (id, region_name) VALUES (%s, %s) RETURNING id",
                (region_id, country)
            )
            return cursor.fetchone()[0]
        return result[0]

    async def _update_or_create_vehicle(self, cursor, vin: str, fleet_id: str, region_id: str, 
                                      vehicle_model_id: str, vehicle_data: pd.Series) -> None:
        """Update or create Vehicle record."""
        end_of_contract = convert_date_format(vehicle_data['end_of_contract'])
        start_date = convert_date_format(vehicle_data['start_date'])
        
        cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vin,))
        vehicle_exists = cursor.fetchone()
        
        if vehicle_exists:
            cursor.execute("""
                UPDATE vehicle 
                SET fleet_id = %s,
                    region_id = %s,
                    vehicle_model_id = %s,
                    licence_plate = %s,
                    end_of_contract_date = %s,
                    start_date = %s,
                    activation_status = %s,
                    is_displayed = %s
                WHERE vin = %s
            """, (
                fleet_id,
                region_id,
                vehicle_model_id,
                vehicle_data['licence_plate'],
                end_of_contract,
                start_date,
                vehicle_data.get('activation_status'),
                vehicle_data.get('is_displayed'),
                vin
            ))
        else:
            vehicle_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle (
                    id, vin, fleet_id, region_id, vehicle_model_id,
                    licence_plate, end_of_contract_date, start_date, activation_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                vehicle_id,
                vin,
                fleet_id,
                region_id,
                vehicle_model_id,
                vehicle_data['licence_plate'],
                end_of_contract,
                start_date,
                vehicle_data.get('activation_status'),
                vehicle_data.get('is_displayed')
            ))
            logging.info(f"New vehicle inserted in DB VIN: {vin}") 
    
    async def _update_or_create_tesla_models(self, cursor, model_name: str, type: str, version: str, make_id: str, oem_id: str, warranty_km: int, warranty_date: str) -> str:
        """Get a Tesla model if it exists then update it, or create it if it doesn't exist."""
        cursor.execute(
            "SELECT id FROM vehicle_model WHERE LOWER(version) = %s",
            (version.lower(),))
        result = cursor.fetchone()
        
        if result:
            model_id = result[0]
            cursor.execute("""
                UPDATE vehicle_model 
                SET model_name = %s, 
                    type = %s, 
                    version = %s, 
                    make_id = %s, 
                    oem_id = %s, 
                    warranty_km = %s, 
                    warranty_date = %s 
                WHERE id = %s
            """, (model_name, type, version, make_id, oem_id, warranty_km, warranty_date, model_id))
            logging.info(f"Updated existing Tesla model with version {version}")
        else:
            model_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle_model (
                    id, model_name, type, version, make_id, oem_id, warranty_km, warranty_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (model_id, model_name, type, version, make_id, oem_id, warranty_km, warranty_date))
            logging.info(f"Created new Tesla model with version {version}")
            
        return model_id

    async def _update_or_create_other_models(self, cursor, model_name: str, type: str, version: str, make_id: str, oem_id: str) -> str:
        """Get a model if it exists then update it, or create it if it doesn't exist."""
        cursor.execute("SELECT id FROM vehicle_model WHERE LOWER(model_name) = %s AND LOWER(type) = %s", (model_name.lower(), type.lower()))
        result = cursor.fetchone()
        if result:
            model_id = result[0]
            cursor.execute("""
                UPDATE vehicle_model 
                SET model_name = %s, 
                    type = %s, 
                    version = %s, 
                    make_id = %s, 
                    oem_id = %s
                WHERE id = %s
            """, (model_name, type, version, make_id, oem_id, model_id))
            logging.info(f"Updated existing model with version {version}")
        else:
            model_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle_model (
                    id, model_name, type, version, make_id, oem_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (model_id, model_name, type, version, make_id, oem_id))
            logging.info(f"Created new model with version {version}")
            
        return model_id
    
    async def process_tesla_vehicles(self, session: aiohttp.ClientSession) -> None:
        """Process Tesla vehicles."""
        try:
            tesla_df = self.df[self.df['brand'] == 'tesla'] and self.df['real_activation'] == True
            for _, vehicle in tesla_df.iterrows():
                model_name,version,type = await self.tesla_api.get_vehicle_options(session, tesla_df['vin'])
                warranty_km,warranty_date,start_date = await self.tesla_api.get_warranty_info(session, tesla_df['vin'])
                fleet_id = await self._get_fleet_id(cursor, tesla_df['owner'])
                oem_id = await self._get_or_create_oem(cursor, tesla_df['oem'])
                make_id = await self._get_or_create_make(cursor, tesla_df['make'], oem_id)
                region_id = await self._get_or_create_region(cursor, tesla_df['country'])
                model_id = await self._get_or_create_model(cursor, model_name, type, make_id, oem_id)

            with get_connection() as con:
                cursor = con.cursor()
                for index, vehicle in tesla_df.iterrows():

                    cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vehicle['vin'],))
                    vehicle_exists = cursor.fetchone()
                    if not vehicle_exists:
                        cursor.execute("UPDATE vehicle SET activation_status = %s WHERE vin = %s", (vehicle['real_activation'], vehicle['vin']))
                        
        except Exception as e:
            logging.error(f"Error processing Tesla vehicle: {str(e)}")
            raise
    

        
        
        

        
        

