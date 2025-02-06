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
    def __init__(self, activation_service: VehicleActivationService):
        self.activation_service = activation_service

    async def process_vehicles(self, df: pd.DataFrame) -> None:
        """Process vehicles from DataFrame and insert into database.
        
        Args:
            df (pd.DataFrame): DataFrame containing vehicle information
        """
        logging.info(f"Starting processing of {len(df)} vehicles")
        processed_count = 0
        error_count = 0
        skipped_count = 0
        
        timeout = aiohttp.ClientTimeout(total=API_TIMEOUT)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                with get_connection() as con:
                    con.autocommit = False
                    cursor = con.cursor()
                    
                    cursor.execute("SAVEPOINT initial_state")
                    
                    for index, vehicle in df.iterrows():
                        try:
                            logging.info(f"Processing vehicle {index + 1}/{len(df)} - VIN: {vehicle['vin']}")
                            cursor.execute(f"SAVEPOINT vehicle_{index}")
                            
                            # Check for interruption
                            if asyncio.current_task().cancelled():
                                logging.info("Processing cancelled. Rolling back transaction...")
                                con.rollback()
                                return
                            
                            # Validate vehicle data
                            is_valid, error_message = validate_vehicle_data(vehicle)
                            if not is_valid:
                                logging.warning(f"Vehicle skipped: {error_message}")
                                skipped_count += 1
                                continue
                                
                            # Process activation status
                            await self.activation_service.process_vehicle_activation(session, vehicle)
                            
                            # Process database updates
                            await self._process_database_updates(cursor, vehicle)
                            
                        except Exception as e:
                            error_count += 1
                            logging.error(f"Error processing vehicle {vehicle.get('vin', 'Unknown VIN')}: {str(e)}")
                            cursor.execute(f"ROLLBACK TO SAVEPOINT vehicle_{index}")
                            continue
                        else:
                            processed_count += 1
                            cursor.execute(f"RELEASE SAVEPOINT vehicle_{index}")
                            if processed_count % 10 == 0:
                                logging.info(f"Progress: {processed_count}/{len(df)} vehicles processed")
                                con.commit()
                                cursor.execute("SAVEPOINT initial_state")
                    
                    try:
                        con.commit()
                        logging.info(f"Processing completed. {processed_count} vehicles processed successfully, {error_count} errors, {skipped_count} skipped.")
                    except Exception as e:
                        logging.error(f"Error during final commit: {str(e)}")
                        con.rollback()
                        raise
                        
        except asyncio.CancelledError:
            logging.info("Processing cancelled. Rolling back any pending changes...")
            if 'con' in locals() and con:
                con.rollback()
            raise
            
        except Exception as e:
            logging.error(f"Error in process_vehicles: {str(e)}")
            if 'con' in locals() and con:
                con.rollback()
            raise

    async def _process_database_updates(self, cursor, vehicle: pd.Series) -> None:
        """Process database updates for a single vehicle."""
        # Handle OEM
        oem_id = await self._get_or_create_oem(cursor, vehicle['oem'])
        
        # Handle Make
        make_id = await self._get_or_create_make(cursor, vehicle['make'], oem_id)
        
        # Handle Model
        vehicle_model_id = await self._get_or_create_model(
            cursor, 
            vehicle['model'],
            vehicle.get('type'),
            make_id,
            oem_id
        )
        
        # Handle Fleet
        fleet_id = await self._get_fleet_id(cursor, vehicle['owner'])
        if not fleet_id:
            raise ValueError(f"Fleet not found for owner: {vehicle['owner']}")
            
        # Handle Region
        region_id = await self._get_or_create_region(cursor, vehicle['country'])
        
        # Handle Vehicle
        await self._update_or_create_vehicle(
            cursor,
            vehicle['vin'],
            fleet_id,
            region_id,
            vehicle_model_id,
            vehicle
        )

    async def _get_or_create_oem(self, cursor, oem_raw: str) -> str:
        """Get or create OEM record."""
        oem_lower = OEM_MAPPING.get(oem_raw, oem_raw.lower())
        
        cursor.execute(
            "SELECT id FROM oem WHERE LOWER(oem_name) = %s",
            (oem_lower,)
        )
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

    async def _get_or_create_model(self, cursor, model_name: str, type_value: Optional[str], make_id: str, oem_id: str) -> str:
        """Get or create Model record."""
        model_name = model_name.strip().lower() if model_name else None
        type_value = type_value.strip().lower() if type_value else None
        
        if not model_name:
            raise ValueError("Model name is required")
            
        cursor.execute("""
            SELECT id FROM vehicle_model 
            WHERE LOWER(model_name) = %s 
            AND (
                (LOWER(type) = %s AND %s IS NOT NULL)
                OR (type IS NULL AND %s IS NULL)
            )
            AND make_id = %s AND oem_id = %s
        """, (model_name, type_value, type_value, type_value, make_id, oem_id))
        
        result = cursor.fetchone()
        if result:
            return result[0]
            
        model_id = str(uuid.uuid4())
        if type_value:
            cursor.execute("""
                INSERT INTO vehicle_model (id, model_name, type, make_id, oem_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (model_id, model_name, type_value, make_id, oem_id))
        else:
            cursor.execute("""
                INSERT INTO vehicle_model (id, model_name, make_id, oem_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (model_id, model_name, make_id, oem_id))
            
        return cursor.fetchone()[0]

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
                    activation_status = %s
                WHERE vin = %s
            """, (
                fleet_id,
                region_id,
                vehicle_model_id,
                vehicle_data['licence_plate'],
                end_of_contract,
                start_date,
                vehicle_data.get('activation_status'),
                vin
            ))
            logging.info(f"Vehicle updated with VIN: {vin}")
        else:
            vehicle_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO vehicle (
                    id, vin, fleet_id, region_id, vehicle_model_id,
                    licence_plate, end_of_contract_date, start_date, activation_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                vehicle_id,
                vin,
                fleet_id,
                region_id,
                vehicle_model_id,
                vehicle_data['licence_plate'],
                end_of_contract,
                start_date,
                vehicle_data.get('activation_status')
            ))
            logging.info(f"New vehicle inserted with VIN: {vin}") 
