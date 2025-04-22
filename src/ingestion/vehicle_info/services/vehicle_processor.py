import logging
import asyncio
import aiohttp
import pandas as pd
import uuid
from typing import Optional, Dict, Any

from ..config.settings import API_TIMEOUT
from ..config.mappings import MAKE_MAPPING, OEM_MAPPING, COUNTRY_MAPPING, TESLA_MODEL_MAPPING
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
    
    async def _get_or_create_tesla_models(self, cursor, model_name: str, type: str, version: str, make: str, oem: str, warranty_km: int, warranty_date: str) -> str:
        """Get a Tesla model if it exists then update it, or create it if it doesn't exist."""
        cursor.execute(
            "SELECT id FROM vehicle_model WHERE LOWER(version) = %s",(version.lower(),))
        result = cursor.fetchone()
        oem_id = await self._get_or_create_oem(cursor, oem)
        make_id = await self._get_or_create_make(cursor, make, oem_id)
        if result:
            model_id = result[0]
            cursor.execute("""
                UPDATE vehicle_model 
                SET warranty_km = COALESCE(warranty_km, %s),
                    warranty_date = COALESCE(warranty_date, %s)
                WHERE id = %s
            """, (warranty_km, warranty_date, model_id))
            logging.info(f"Updated existing Tesla model with version {version}")
            return model_id
        else:
            model_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle_model (
                    id, model_name, type, version, make_id, oem_id, warranty_km, warranty_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (model_id, model_name, type, version, make_id, oem_id, warranty_km, warranty_date))
            logging.info(f"Created new Tesla model with version {version}")
            
        return model_id

    async def _update_or_create_other_models(self, cursor, model_name: str, type: str, make: str, oem: str) -> str:
        """Get a model if it exists then update it, or create it if it doesn't exist."""
        if not model_name or model_name.strip() == '':
            model_name = 'unknown'
        if not type or type.strip() == '':
            type = 'unknown'
        cursor.execute("SELECT id FROM vehicle_model WHERE LOWER(model_name) = %s AND LOWER(type) = %s", (model_name.lower(), type.lower()))
        result = cursor.fetchone()
        oem_id = await self._get_or_create_oem(cursor, oem)
        make_id = await self._get_or_create_make(cursor, make, oem_id)
        if result:
            model_id = result[0]
            cursor.execute("""
                UPDATE vehicle_model 
                SET model_name = %s, 
                    type = %s,  
                    make_id = %s, 
                    oem_id = %s
                WHERE id = %s
            """, (model_name, type, make_id, oem_id, model_id))
            logging.info(f"Updated existing model with name {model_name} and type {type}")
        else:
            model_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle_model (
                    id, model_name, type, make_id, oem_id
                ) VALUES (%s, %s, %s, %s, %s)
            """, (model_id, model_name, type, make_id, oem_id))
            logging.info(f"Created new model with name {model_name} and type {type}")
            
        return model_id
    
    async def process_tesla(self) -> None:
        """Process Tesla vehicles."""
        try:
            tesla_df = self.df[(self.df['oem'] == 'tesla') & (self.df['real_activation'] == True)]
                
            async with aiohttp.ClientSession() as session:
                with get_connection() as con:
                    cursor = con.cursor()
                    for _, vehicle in tesla_df.iterrows():
                        try:
                            vin = vehicle['vin']
                            model_code = vin[3]
                            model_name = TESLA_MODEL_MAPPING.get(model_code, 'unknown')
                            logging.info(f"Processing Tesla vehicle {vin} with model {model_name}")
                            fleet_id = await self._get_fleet_id(cursor, vehicle['owner'])
                            region_id = await self._get_or_create_region(cursor, vehicle['country'])
                            # Check if vehicle exists
                            cursor.execute("SELECT id, vehicle_model_id FROM vehicle WHERE vin = %s", (vin,))
                            result = cursor.fetchone()

                            if not result:
                                # CASE 1: Vehicle doesn't exist
                                # Get model info from API
                                version, type = await self.tesla_api.get_vehicle_options(session, vin, model_name)
                                warranty_km, warranty_date, start_date = await self.tesla_api.get_warranty_info(session, vin)
                                
                                # Create/get model and related records
                                model_id = await self._get_or_create_tesla_models(cursor, model_name, type, version, vehicle['make'], vehicle['oem'], warranty_km, warranty_date)
                                
                                # Insert new vehicle
                                vehicle_id = str(uuid.uuid4())
                                await self._insert_tesla_vehicle(cursor, vehicle_id, vin, model_id, fleet_id, region_id, vehicle['licence_plate'], vehicle['end_of_contract'], start_date, vehicle['real_activation'], vehicle['EValue'])
                            else:
                                # CASE 2: Vehicle exists
                                vehicle_id = result[0]
                                model_id = result[1]
                                await self._update_activation_status_and_is_displayed(cursor, vehicle_id, vehicle['real_activation'], vehicle['EValue'])
                                # Check current model version
                                cursor.execute("SELECT version FROM vehicle_model WHERE id = %s", (model_id,))
                                current_version = cursor.fetchone()[0]
                                
                                if current_version == 'MTU':
                                    # CASE 2.2: Current version is unknown, check API
                                    api_version, type = await self.tesla_api.get_vehicle_options(session, vehicle['vin'], model_name)
                                    
                                    if api_version != 'MTU':
                                        # Only update if API returns a known version
                                        warranty_km, warranty_date, start_date = await self.tesla_api.get_warranty_info(session, vehicle['vin'])
                                        new_model_id = await self._get_or_create_tesla_models(cursor, model_name, type, api_version, vehicle['make'], vehicle['oem'], warranty_km, warranty_date)
                                        
                                        # Update vehicle with new model
                                        cursor.execute("UPDATE vehicle SET vehicle_model_id = %s WHERE id = %s", (new_model_id, vehicle_id))
                                else:
                                    warranty_km, warranty_date, start_date = await self.tesla_api.get_warranty_info(session, vin)
                                    cursor.execute("""
                                        UPDATE vehicle_model 
                                        SET warranty_km = COALESCE(warranty_km, %s),
                                            warranty_date = COALESCE(warranty_date, %s)
                                        WHERE id = %s
                                    """, (warranty_km, warranty_date, model_id))

                            con.commit()
                        except Exception as e:
                            logging.error(f"Error processing Tesla vehicle {vehicle['vin']}: {str(e)}")
                            con.rollback()
                            continue
        except Exception as e:
            logging.error(f"Error in Tesla processing: {str(e)}")
            raise

    async def _update_activation_status_and_is_displayed(self, cursor, vehicle_id: str, activation_status: bool, is_displayed: bool) -> None:
        """Update activation status and is_displayed for a vehicle."""
        cursor.execute(
            "UPDATE vehicle SET activation_status = %s, is_displayed = %s WHERE id = %s",
            (activation_status, is_displayed, vehicle_id)
        )

    async def _insert_tesla_vehicle(self, cursor, vehicle_id: str, vin: str, model_id: str, fleet_id: str, region_id: str, licence_plate: str, end_of_contract: str, start_date: str, activation_status: bool, is_displayed: bool) -> None:
        """Insert a Tesla vehicle into the database."""
        cursor.execute(
            "INSERT INTO vehicle (id, vin, fleet_id, region_id, vehicle_model_id, licence_plate, end_of_contract_date, start_date, activation_status, is_displayed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (vehicle_id, vin, fleet_id, region_id, model_id, licence_plate, end_of_contract, start_date, activation_status, is_displayed)
        )
        logging.info(f"New Tesla vehicle inserted in DB VIN: {vin}")

    async def process_other_vehicles(self) -> None:
        """Process other vehicles."""
        try:
            other_df = self.df[(self.df['oem'] != 'tesla') & (self.df['real_activation'] == True)]
            print(other_df)
            with get_connection() as con:
                cursor = con.cursor()
                for _, vehicle in other_df.iterrows():
                    try:
                        cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vehicle['vin'],))
                        vehicle_exists = cursor.fetchone()
                        fleet_id = await self._get_fleet_id(cursor, vehicle['owner'])
                        region_id = await self._get_or_create_region(cursor, vehicle['country'])
                        model_id = await self._update_or_create_other_models(cursor, vehicle['model'],vehicle['type'], vehicle['make'], vehicle['oem'])
                        if not vehicle_exists:
                            vehicle_id = str(uuid.uuid4())
                            insert_query = """
                                INSERT INTO vehicle (
                                    id, vin, fleet_id, region_id, vehicle_model_id,
                                    licence_plate, end_of_contract_date, start_date, activation_status, is_displayed
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(
                                insert_query,
                                (
                                    vehicle_id, vehicle['vin'], fleet_id, region_id, model_id,
                                    vehicle['licence_plate'], vehicle['end_of_contract'],
                                    vehicle['start_date'], vehicle['real_activation'], vehicle['EValue']
                                )
                            )
                            logging.info(f"New vehicle inserted in DB VIN: {vehicle['vin']}")
                        else:
                            cursor.execute(
                                "UPDATE vehicle SET vehicle_model_id = %s, activation_status = %s, is_displayed = %s WHERE vin = %s",
                                (model_id, vehicle['real_activation'], vehicle['EValue'], vehicle['vin'])
                            )
                            logging.info(f"Updated vehicle in DB VIN: {vehicle['vin']}")
                            
                        con.commit()
                    except Exception as e:
                        logging.error(f"Error processing vehicle {vehicle['vin']}: {str(e)}")
                        con.rollback()
                        continue
        except Exception as e:
            logging.error(f"Error in other vehicles processing: {str(e)}")
            raise

    async def process_deactivated_vehicles(self) -> None:
        """Process deactivated vehicles."""
        logging.info(f"Processing deactivated vehicles")
        deactivated_df = self.df[self.df['real_activation'] == False]
        if deactivated_df.empty:
            logging.info("No deactivated vehicles to process")
            return

        try:
            with get_connection() as con:
                cursor = con.cursor()
                
                # Create a temporary table for bulk operations
                cursor.execute("""
                    CREATE TEMPORARY TABLE temp_deactivated_vehicles (
                        vin VARCHAR(17) PRIMARY KEY
                    ) ON COMMIT DROP
                """)
                
                # Bulk insert VINs into temporary table
                vins_to_update = deactivated_df['vin'].tolist()
                cursor.executemany(
                    "INSERT INTO temp_deactivated_vehicles (vin) VALUES (%s)",
                    [(vin,) for vin in vins_to_update]
                )
                
                # Perform the update using a join with the temporary table
                cursor.execute("""
                    UPDATE vehicle v
                    SET activation_status = false,
                        is_displayed = false,
                        updated_at = CURRENT_TIMESTAMP
                    FROM temp_deactivated_vehicles t
                    WHERE v.vin = t.vin
                """)
                
                affected_rows = cursor.rowcount
                logging.info(f"Bulk updated {affected_rows} deactivated vehicles in DB")
                con.commit()
                
        except Exception as e:
            logging.error(f"Error in deactivated vehicles processing: {str(e)}")
            if 'con' in locals():
                con.rollback()
            raise
            
        

        
        

