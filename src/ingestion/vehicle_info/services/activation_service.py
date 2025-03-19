import logging
import asyncio
import aiohttp
from typing import Tuple, Optional
import pandas as pd
import requests
import uuid

from ..api.bmw_client import BMWApi
from ..api.hm_client import HMApi
from ..api.stellantis_client import StellantisApi
from ..api.tesla_client import TeslaApi
from ..config.settings import ACTIVATION_TIMEOUT
from ..services.google_sheet_service import update_google_sheet_status
from core.sql_utils import get_connection

class VehicleActivationService:
    _ayvens_fleet_id = None  # Cache for Ayvens fleet ID, not necessary

    def __init__(self, bmw_api: BMWApi, hm_api: HMApi, stellantis_api: StellantisApi, tesla_api: TeslaApi, fleet_info_df: pd.DataFrame):
        self.bmw_api = bmw_api
        self.hm_api = hm_api
        self.stellantis_api = stellantis_api
        self.tesla_api = tesla_api
        self.fleet_info_df = fleet_info_df
        self.db_url = None

    async def _get_vehicle_ownership(self, vin: str) -> str:
        """Get the ownership of a vehicle from fleet info.
        
        Args:
            vin: Vehicle VIN
            
        Returns:
            The ownership of the vehicle, or 'bib' as default if not found
        """
        try:
            if self.fleet_info_df is None:
                logging.warning("Fleet info DataFrame not set, defaulting to 'bib'")
                return 'bib'
            
            vehicle_info = self.fleet_info_df[self.fleet_info_df['vin'] == vin]
            if not vehicle_info.empty:
                return vehicle_info['owner'].iloc[0]
            
            logging.warning(f"Vehicle {vin} not found in fleet info, defaulting to 'bib'")
            return 'bib'
            
        except Exception as e:
            logging.error(f"Error getting vehicle ownership: {str(e)}")
            return 'bib'

    async def activate_bmw(self, session: aiohttp.ClientSession, vin: str) -> Tuple[bool, Optional[str]]:
        """Activate a BMW vehicle using BMW's API."""
        try:
            license_plate = ""
            end_date = ""
            
            if self.fleet_info_df is not None:
                vehicle_info = self.fleet_info_df[self.fleet_info_df['vin'] == vin]
                if not vehicle_info.empty:
                    license_plate = str(vehicle_info['licence_plate'].iloc[0]) if 'licence_plate' in vehicle_info.columns else ""
                    end_date = str(vehicle_info['end_of_contract'].iloc[0]) if 'end_of_contract' in vehicle_info.columns else ""
                    
                    license_plate = "" if pd.isna(license_plate) else license_plate
                    end_date = "" if pd.isna(end_date) else end_date
                    
                    logging.info(f"Found vehicle info - VIN: {vin}, License Plate: {license_plate}, End Date: {end_date}")
                else:
                    logging.warning(f"No vehicle info found in fleet_info for VIN: {vin}")
            else:
                logging.warning("fleet_info_df is None, using empty values for license_plate and end_date")
            
            payload = {
                "vin": vin,
                "licence_plate": license_plate,
                "note": "",
                "contract": {
                    "end_date": end_date,
                }
            }
            
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.create_clearance(payload)
            )
            
            logging.info(f"Create clearance response - Status: {status_code}, Result: {result}")
            
            if status_code in [200, 201, 204]:
                fleet_success, fleet_error = await self._add_to_fleet(vin)
                if not fleet_success:
                    return False, fleet_error
                return True, None
            else:
                error_msg = f"Failed to activate BMW vehicle: HTTP {status_code}, Response: {result}"
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error activating BMW vehicle: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    async def activate_high_mobility(self, session: aiohttp.ClientSession, vin: str, make: str) -> Tuple[bool, Optional[str]]:
        """Activate a vehicle using High Mobility's API.
        
        Args:
            session: aiohttp client session
            vin: Vehicle VIN
            make: Vehicle make/brand
        """
        try:
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.hm_api.create_clearance([{"vin": vin, "brand": make}])
            )
            
            if status_code in [200, 201, 204]:
                return True, None
            else:
                error_msg = f"Failed to activate High Mobility vehicle: HTTP {status_code}"
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error activating High Mobility vehicle: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    async def _add_to_fleet(self, vin: str) -> Tuple[bool, Optional[str]]:
        """Add a vehicle to the appropriate fleet based on ownership.
        
        Args:
            vin: Vehicle VIN
        """
        try:
            target_fleet_name = await self._get_vehicle_ownership(vin)
            
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.get_fleets()
            )
            
            if status_code != 200:
                error_msg = f"Failed to get fleets: HTTP {status_code}"
                logging.error(error_msg)
                return False, error_msg
            
            target_fleet_id = None
            for fleet in result.get('fleets', []):
                if fleet.get('name', '').lower() == target_fleet_name.lower():
                    target_fleet_id = fleet['fleet_id']
                    break
                    
            if not target_fleet_id:
                error_msg = f"Fleet {target_fleet_name} not found"
                logging.error(error_msg)
                return False, error_msg
            
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.add_vehicle_to_fleet(target_fleet_id, vin)
            )
            
            if status_code in [200, 201, 204]:
                logging.info(f"Successfully added vehicle {vin} to {target_fleet_name} fleet")
                return True, None
            else:
                error_msg = f"Failed to add vehicle to fleet: HTTP {status_code}"
                logging.error(error_msg)
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error adding vehicle to fleet: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    async def deactivate_bmw(self, session: aiohttp.ClientSession, vin: str) -> bool:
        """Deactivate a BMW vehicle."""
        try:            
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.delete_clearance(vin)
            )
            
            success = status_code in [200, 204]
            if not success:
                logging.error(f"Failed to deactivate BMW vehicle {vin}: HTTP {status_code}")
            return success
            
        except Exception as e:
            logging.error(f"Error deactivating BMW vehicle {vin}: {str(e)}")
            return False

    async def deactivate_high_mobility(self, session: aiohttp.ClientSession, vin: str) -> bool:
        """Deactivate a High Mobility vehicle."""
        try:
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.hm_api.get_status(vin)
            )
            
            if status_code == 404:
                logging.info(f"High Mobility vehicle {vin} not found (already deactivated)")
                return True
                
            if status_code != 200:
                logging.error(f"Failed to check High Mobility vehicle status: HTTP {status_code}")
                return False
                
            current_status = result.get('status', '').lower()
            if current_status in ['revoked', 'rejected']:
                logging.info(f"High Mobility vehicle {vin} already deactivated (status: {current_status})")
                return True
                
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.hm_api.delete_clearance(vin)
            )
            
            success = status_code in [200, 204]
            if not success:
                logging.error(f"Failed to deactivate High Mobility vehicle {vin}: HTTP {status_code}")
            else:
                await update_google_sheet_status(vin, False, None, None, "TRUE")
                await self._update_vehicle_in_db(vin, False, True, {})
            return success
            
        except Exception as e:
            logging.error(f"Error deactivating High Mobility vehicle {vin}: {str(e)}")
            return False

    async def _process_bmw_vehicle(self, session: aiohttp.ClientSession, vehicle: dict, desired_state: bool) -> None:
        """Process BMW vehicle activation/deactivation."""
        vin = vehicle['vin']
        logging.info(f"Processing BMW vehicle - VIN: {vin}")
        
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.bmw_api.check_vehicle_status(vin)
        )
        
        current_state = False
        if status_code == 200:
            current_state = True
            logging.info(f"BMW is 200 - VIN: {vin}, Active: {current_state}")
        elif status_code == 404:
            current_state = False
            logging.info(f"BMW is 404 - VIN: {vin}, Active: {current_state}")
        else:
            error_msg = f"Failed to check vehicle status: HTTP {status_code}"
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vin, None, error_msg)
            await self._update_vehicle_in_db(vin, False, True, vehicle)
            return
        
        if current_state == desired_state:
            logging.info(f"BMW vehicle {vin} already in desired state: {desired_state}")
            vehicle['activation_status'] = str(desired_state)
            await update_google_sheet_status(vin, current_state, None, None, "TRUE")
            await self._update_vehicle_in_db(vin, current_state, True, vehicle)
            return
        
        if desired_state:
            logging.info(f"Attempting to activate BMW vehicle - VIN: {vin}")
            success, error_msg = await asyncio.wait_for(
                self.activate_bmw(session, vin),
                timeout=ACTIVATION_TIMEOUT
            )
            if success:
                await update_google_sheet_status(vin, True, None, None, "TRUE")
                await self._update_vehicle_in_db(vin, True, True, vehicle)
            else:
                await update_google_sheet_status(vin, False, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vin, False, True, vehicle)
        else:
            logging.info(f"Attempting to deactivate BMW vehicle - VIN: {vin}")
            try:
                deactivation_success = await asyncio.wait_for(
                    self.deactivate_bmw(session, vin),
                    timeout=ACTIVATION_TIMEOUT
                )
                
                if deactivation_success:
                    status_code, _ = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.bmw_api.check_vehicle_status(vin)
                    )
                    
                    if status_code == 404:
                        await update_google_sheet_status(vin, False, None, None, "TRUE")
                        await self._update_vehicle_in_db(vin, False, True, vehicle)
                    else:
                        error_msg = f"Deactivation seemed successful but vehicle is still active (status: {status_code})"
                        await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
                        await self._update_vehicle_in_db(vin, True, True, vehicle)
                else:
                    error_msg = "Failed to deactivate BMW vehicle - API returned failure"
                    await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
                    await self._update_vehicle_in_db(vin, True, True, vehicle)
            except Exception as e:
                error_msg = f"Failed to deactivate BMW vehicle - Error: {str(e)}"
                logging.error(error_msg)
                await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vin, True, True, vehicle)

    async def _process_high_mobility_vehicle(self, session: aiohttp.ClientSession, vehicle: dict, desired_state: bool) -> None:
        """Process High Mobility vehicle activation/deactivation."""
        vin = vehicle['vin']
        make_lower = vehicle['make'].lower()
        logging.info(f"Processing High Mobility vehicle - VIN: {vin}, Make: {make_lower}")
        
        try:
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.hm_api.get_status(vin)
            )
            
            current_state = False
            if status_code == 200:
                current_state = result.get('status') == "ACTIVE"
                logging.info(f"High Mobility vehicle current state - VIN: {vin}, Active: {current_state}")
            elif status_code == 401:
                error_msg = "Authentication failed with High Mobility API - Token might be expired"
                logging.error(error_msg)
                vehicle['activation_status'] = 'false'
                await update_google_sheet_status(vin, None, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vin, False, True, vehicle)
                return
            elif status_code == 404:
                current_state = False
                logging.info(f"High Mobility vehicle not found (404) - VIN: {vin}, considering as inactive")
            else:
                error_msg = f"Failed to check vehicle status: HTTP {status_code}, Response: {result}"
                logging.error(error_msg)
                vehicle['activation_status'] = 'false'
                await update_google_sheet_status(vin, None, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vin, False, True, vehicle)
                return
                
            if current_state == desired_state:
                logging.info(f"High Mobility vehicle {vin} already in desired state: {desired_state}")
                vehicle['activation_status'] = str(desired_state)
                await update_google_sheet_status(vin, current_state, None, None, "TRUE")
                await self._update_vehicle_in_db(vin, current_state, True, vehicle)
                return
                
            if desired_state:
                logging.info(f"Attempting to activate High Mobility vehicle - VIN: {vin}")
                try:
                    success, error_msg = await asyncio.wait_for(
                        self.activate_high_mobility(session, vin, make_lower),
                        timeout=ACTIVATION_TIMEOUT
                    )
                    if success:
                        await update_google_sheet_status(vin, True, None, None, "TRUE")
                        await self._update_vehicle_in_db(vin, True, True, vehicle)
                    else:
                        await update_google_sheet_status(vin, False, error_msg, None, "TRUE")
                        await self._update_vehicle_in_db(vin, False, True, vehicle)
                except Exception as e:
                    error_msg = f"Error activating High Mobility vehicle: {str(e)}"
                    logging.error(error_msg)
                    await update_google_sheet_status(vin, False, error_msg, None, "TRUE")
                    await self._update_vehicle_in_db(vin, False, True, vehicle)
            else:
                logging.info(f"Attempting to deactivate High Mobility vehicle - VIN: {vin}")
                try:
                    success = await asyncio.wait_for(
                        self.deactivate_high_mobility(session, vin),
                        timeout=ACTIVATION_TIMEOUT
                    )
                    if not success:
                        error_msg = "Failed to deactivate High Mobility vehicle"
                        await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
                        await self._update_vehicle_in_db(vin, True, True, vehicle)
                    else:
                        await update_google_sheet_status(vin, False, None, None, "TRUE")
                        await self._update_vehicle_in_db(vin, False, True, vehicle)
                except Exception as e:
                    error_msg = f"Error deactivating High Mobility vehicle: {str(e)}"
                    logging.error(error_msg)
                    await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
                    await self._update_vehicle_in_db(vin, True, True, vehicle)
                    
        except Exception as e:
            error_msg = f"Error processing High Mobility vehicle: {str(e)}"
            logging.error(error_msg)
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vin, None, error_msg, None, "TRUE")
            await self._update_vehicle_in_db(vin, False, True, vehicle)

    async def _process_stellantis_vehicle(self, session: aiohttp.ClientSession, vehicle: dict, desired_state: bool, eligibility: str) -> None:
        """Process Stellantis vehicle activation/deactivation."""
        vin = vehicle['vin']
        make_lower = vehicle['make'].lower()
        logging.info(f"Processing Stellantis vehicle - VIN: {vin}, Make: {make_lower}")
        
        # Check eligibility via API if not already marked as non-eligible
        if not eligibility and desired_state:
            is_eligible = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.stellantis_api.is_eligible(vin)
            )
            if not is_eligible:
                logging.info(f"Vehicle {vin} not eligible via API")
                await self._update_vehicle_in_db(vin, False, False, vehicle)
                await update_google_sheet_status(vin, False, "Vehicle not eligible", False, "FALSE")
                return
        
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.stellantis_api.get_status(vin)
        )
        
        current_state = False
        contract_id = None
        if status_code == 200 and result:
            contract_id = result.get("_id")
            status = result.get("status", "")
            current_state = status == "activated"
            
            if current_state:
                logging.info(f"Stellantis vehicle is activated - VIN: {vin}, Contract ID: {contract_id}")
                await update_google_sheet_status(vin, True, None)
                
                if not desired_state:
                    logging.info(f"Vehicle is activated but deactivation is requested - VIN: {vin}")
                    if contract_id:
                        await self._handle_stellantis_deactivation(vin, contract_id, vehicle)
                    return
                else:
                    vehicle['activation_status'] = 'true'
                    await update_google_sheet_status(vin, True, None, None, "TRUE")
                    await self._update_vehicle_in_db(vin, True, True, vehicle)
                    return
            
            if status:
                if status == "cancelled":
                    if not desired_state:
                        logging.info(f"Vehicle {vin} is already cancelled (deactivated)")
                        vehicle['activation_status'] = 'false'
                        await update_google_sheet_status(vin, False, None, None, "TRUE")
                        await self._update_vehicle_in_db(vin, False, True, vehicle)
                        return
                    else:
                        logging.info(f"Vehicle {vin} is cancelled but activation is requested")
                else:
                    error_msg = f"Contract status is {status}"
                    vehicle['activation_status'] = 'false'
                    await update_google_sheet_status(vin, False, error_msg, None, "TRUE")
                    await self._update_vehicle_in_db(vin, False, True, vehicle)
                    return
                    
            logging.info(f"Stellantis vehicle current state - VIN: {vin}, Status: {status}, Contract ID: {contract_id}")
        elif status_code != 404:
            error_msg = f"Failed to check vehicle status: HTTP {status_code}"
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vin, None, error_msg)
            await self._update_vehicle_in_db(vin, False, True, vehicle)
            return

        if current_state != desired_state:
            if desired_state:
                await self._handle_stellantis_activation(vin, vehicle)
            else:
                if not contract_id:
                    error_msg = f"Cannot deactivate vehicle {vin}: No active contract found"
                    await update_google_sheet_status(vin, False, error_msg, None, "TRUE")
                    await self._update_vehicle_in_db(vin, False, True, vehicle)
                else:
                    await self._handle_stellantis_deactivation(vin, contract_id, vehicle)

    async def _handle_stellantis_activation(self, vin: str, vehicle: dict) -> None:
        """Handle Stellantis vehicle activation."""
        logging.info(f"Attempting to activate Stellantis vehicle - VIN: {vin}")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.stellantis_api.create_clearance([{"vin": vin}])
        )
        if status_code in [200, 201, 204]:
            await update_google_sheet_status(vin, True, None, None, "TRUE")
            await self._update_vehicle_in_db(vin, True, True, vehicle)
        else:
            if status_code == 409:
                error_msg = "Vehicle already has an active contract or activation in progress"
                await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vin, True, True, vehicle)
            else:
                error_msg = f"Failed to activate Stellantis vehicle: HTTP {status_code}"
                await update_google_sheet_status(vin, False, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vin, False, True, vehicle)

    async def _handle_stellantis_deactivation(self, vin: str, contract_id: str, vehicle: dict) -> None:
        """Handle Stellantis vehicle deactivation."""
        logging.info(f"Attempting to deactivate Stellantis vehicle - VIN: {vin}, Contract: {contract_id}")
        status_code, result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.stellantis_api.delete_clearance(contract_id)
        )
        if status_code in [200, 204]:
            await update_google_sheet_status(vin, False, None, None, "TRUE")
            await self._update_vehicle_in_db(vin, False, True, vehicle)
        else:
            error_msg = f"Failed to deactivate Stellantis vehicle: HTTP {status_code}"
            await update_google_sheet_status(vin, True, error_msg, None, "TRUE")
            await self._update_vehicle_in_db(vin, True, True, vehicle)

    async def _process_tesla_vehicle(self, session: aiohttp.ClientSession, vehicle: dict, desired_state: bool) -> None:
        """Process Tesla vehicle activation/deactivation.
        
        For Tesla vehicles:
        - If vehicle not found in Tesla API:
            - Set activation_status to False in DB
            - Set Real Activation to False in Google Sheet
        - If vehicle exists in DB:
            - If EValue is False in Google Sheet:
                - Set activation_status to False and is_displayed to False in DB
            - Otherwise:
                - Set activation_status and is_displayed based on Tesla API check
        - If vehicle doesn't exist in DB:
            - Set all statuses to False
        """
        vin = vehicle['vin']
        logging.info(f"Processing Tesla vehicle - VIN: {vin}")
        
        with get_connection() as con:
            cursor = con.cursor()
            cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vin,))
            vehicle_exists = cursor.fetchone() is not None
        
        evalue = str(vehicle.get('EValue', '')).upper()
        
        account = await self.tesla_api.get_account_for_vin(session, vin)
        if not account:
            logging.info(f"Tesla vehicle {vin} not found in any Tesla account")
            await update_google_sheet_status(vin, False, "Vehicle not found in Tesla accounts", None, "TRUE")
            await self._update_vehicle_in_db(vin, False, True, vehicle)
            return
            
        if vehicle_exists:
            if evalue == 'FALSE':
                logging.info(f"Tesla vehicle {vin} has EValue=FALSE, setting all statuses to False")
                await update_google_sheet_status(vin, False, None, None, "FALSE")
                await self._update_vehicle_in_db(vin, False, False, vehicle)
            else:
                logging.info(f"Tesla vehicle {vin} found in Tesla account {account}, setting activation to True")
                await update_google_sheet_status(vin, True, None, None, "TRUE")
                await self._update_vehicle_in_db(vin, True, True, vehicle)
        else:
            logging.info(f"Tesla vehicle {vin} not found in DB, setting all statuses to False")
            await update_google_sheet_status(vin, False, None, None, "FALSE")
            await self._update_vehicle_in_db(vin, False, False, vehicle)

    async def process_vehicle_activation(self, session: aiohttp.ClientSession, vehicle: dict) -> None:
        """Process vehicle activation or deactivation based on make and activation status."""
        make_lower = vehicle['make'].lower()
        desired_state = str(vehicle.get('activation', 'FALSE')).upper() == 'TRUE'
        real_activation_str = str(vehicle.get('real_activation', '')).upper()
        evalue = str(vehicle.get('EValue', '')).upper()
        
        logging.info(f"Processing vehicle - VIN: {vehicle['vin']}, Make: {make_lower}, Desired State: {desired_state}, Real State: {real_activation_str}, EValue: {evalue}")
        
        try:
            with get_connection() as con:
                cursor = con.cursor()
                cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vehicle['vin'],))
                vehicle_exists = cursor.fetchone() is not None
            
            if evalue in ['TRUE', 'FALSE']:
                logging.info(f"Found EValue for vehicle {vehicle['vin']}: {evalue}")
                if evalue == 'FALSE':
                    logging.info(f"Vehicle {vehicle['vin']} marked as not displayed (EValue: FALSE)")
                    if vehicle_exists:
                        await self._update_vehicle_in_db(vehicle['vin'], False, False, vehicle)
                    return
            
            eligibility = str(vehicle.get('Eligibility', '')).upper()
            if eligibility == 'FALSE':
                logging.info(f"Vehicle {vehicle['vin']} marked as not eligible in CSV")
                if vehicle_exists:
                    await self._update_vehicle_in_db(vehicle['vin'], False, False, vehicle)
                await update_google_sheet_status(vehicle['vin'], False, "Vehicle marked as not eligible", False, "FALSE")
                return
            
            if real_activation_str and real_activation_str in ['TRUE', 'FALSE']:
                real_activation = real_activation_str == 'TRUE'
                if desired_state == real_activation:
                    logging.info(f"Skipping API calls for VIN {vehicle['vin']} as desired state matches real activation")
                    vehicle['activation_status'] = str(desired_state)
                    await self._update_vehicle_in_db(vehicle['vin'], desired_state, True, vehicle)
                    return
            
            if make_lower == 'bmw':
                await self._process_bmw_vehicle(session, vehicle, desired_state)
            elif make_lower in ['ford', 'mercedes', 'kia']:
                await self._process_high_mobility_vehicle(session, vehicle, desired_state)
            elif make_lower in ['opel', 'citroën', 'ds', 'fiat', 'peugeot']:
                await self._process_stellantis_vehicle(session, vehicle, desired_state, eligibility)
            elif make_lower == 'tesla':
                await self._process_tesla_vehicle(session, vehicle, desired_state)
            else:
                logging.info(f"Vehicle brand not supported for activation: {make_lower}")
                error_msg = "Brand not supported for activation"
                await update_google_sheet_status(vehicle['vin'], None, error_msg, None, "TRUE")
                await self._update_vehicle_in_db(vehicle['vin'], False, True, vehicle)
                
        except asyncio.TimeoutError:
            error_msg = f"Timeout while processing {make_lower} vehicle activation"
            logging.error(error_msg)
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vehicle['vin'], None, error_msg)
            await self._update_vehicle_in_db(vehicle['vin'], False, True, vehicle)
            
        except Exception as e:
            error_msg = f"Error processing {make_lower} vehicle: {str(e)}"
            logging.error(error_msg)
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vehicle['vin'], None, error_msg)
            await self._update_vehicle_in_db(vehicle['vin'], False, True, vehicle)


