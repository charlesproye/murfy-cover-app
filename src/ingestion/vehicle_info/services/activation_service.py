import logging
import asyncio
import aiohttp
from typing import Tuple, Optional
import pandas as pd

from ..api.bmw_client import BMWApi
from ..api.hm_client import HMApi
from ..api.stellantis_client import StellantisApi
from ..config.settings import ACTIVATION_TIMEOUT
from ..services.google_sheet_service import update_google_sheet_status
from ..fleet_info import read_fleet_info

class VehicleActivationService:
    _ayvens_fleet_id = None  # Cache for Ayvens fleet ID

    def __init__(self, bmw_api: BMWApi, hm_api: HMApi, stellantis_api: StellantisApi):
        self.bmw_api = bmw_api
        self.hm_api = hm_api
        self.stellantis_api = stellantis_api
        self.fleet_info_df = None

    def set_fleet_info(self, df: pd.DataFrame) -> None:
        """Set the fleet info DataFrame.
        
        Args:
            df: DataFrame containing fleet information
        """
        self.fleet_info_df = df

    async def _get_vehicle_ownership(self, vin: str) -> str:
        """Get the ownership of a vehicle from fleet info.
        
        Args:
            vin: Vehicle VIN
            
        Returns:
            The ownership of the vehicle, or 'ayvens' as default if not found
        """
        try:
            if self.fleet_info_df is None:
                logging.warning("Fleet info DataFrame not set, defaulting to 'ayvens'")
                return 'ayvens'
            
            vehicle_info = self.fleet_info_df[self.fleet_info_df['vin'] == vin]
            if not vehicle_info.empty:
                return vehicle_info['owner'].iloc[0]
            
            logging.warning(f"Vehicle {vin} not found in fleet info, defaulting to 'ayvens'")
            return 'ayvens'
            
        except Exception as e:
            logging.error(f"Error getting vehicle ownership: {str(e)}")
            return 'ayvens'

    async def activate_bmw(self, session: aiohttp.ClientSession, vin: str) -> Tuple[bool, Optional[str]]:
        """Activate a BMW vehicle using BMW's API."""
        try:
            # Get license plate and end date from fleet info if available
            license_plate = ""
            end_date = ""
            
            if self.fleet_info_df is not None:
                vehicle_info = self.fleet_info_df[self.fleet_info_df['vin'] == vin]
                if not vehicle_info.empty:
                    license_plate = str(vehicle_info['licence_plate'].iloc[0]) if 'licence_plate' in vehicle_info.columns else ""
                    end_date = str(vehicle_info['end_of_contract'].iloc[0]) if 'end_of_contract' in vehicle_info.columns else ""
                    
                    # Convert empty or NaN values to empty string
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
                # After successful clearance creation, add to fleet
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
            # Get vehicle ownership from fleet info
            target_fleet_name = await self._get_vehicle_ownership(vin)
            
            # Get available fleets
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.get_fleets()
            )
            
            if status_code != 200:
                error_msg = f"Failed to get fleets: HTTP {status_code}"
                logging.error(error_msg)
                return False, error_msg
            
            # Find the fleet with matching name
            target_fleet_id = None
            for fleet in result.get('fleets', []):
                if fleet.get('name', '').lower() == target_fleet_name.lower():
                    target_fleet_id = fleet['fleet_id']
                    break
                    
            if not target_fleet_id:
                error_msg = f"Fleet {target_fleet_name} not found"
                logging.error(error_msg)
                return False, error_msg
            
            # Add vehicle to fleet
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
            return success
            
        except Exception as e:
            logging.error(f"Error deactivating High Mobility vehicle {vin}: {str(e)}")
            return False

    async def process_vehicle_activation(self, session: aiohttp.ClientSession, vehicle: dict) -> None:
        """Process vehicle activation or deactivation based on make and activation status."""
        make_lower = vehicle['make'].lower()
        desired_state = str(vehicle.get('activation', 'FALSE')).upper() == 'TRUE'
        real_activation_str = str(vehicle.get('real_activation', '')).upper()
        
        logging.info(f"Processing vehicle - VIN: {vehicle['vin']}, Make: {make_lower}, Desired State: {desired_state}, Real State: {real_activation_str}")
        
        # Skip only if real_activation has an explicit value and matches desired state
        if real_activation_str and real_activation_str in ['TRUE', 'FALSE']:
            real_activation = real_activation_str == 'TRUE'
            if desired_state == real_activation:
                logging.info(f"Skipping API calls for VIN {vehicle['vin']} as desired state matches real activation")
                vehicle['activation_status'] = str(desired_state)
                return
            
        try:
            success = False
            error_msg = None
            should_update_real_activation = False
            status_code = None
            
            if make_lower == 'bmw':
                logging.info(f"Processing BMW vehicle - VIN: {vehicle['vin']}")
                # Always check current status first
                status_code, result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.bmw_api.check_vehicle_status(vehicle['vin'])
                )
                
                current_state = False
                if status_code == 200:
                    current_state = True
                    logging.info(f"BMW is 200 - VIN: {vehicle['vin']}, Active: {current_state}")
                elif status_code == 404:
                    current_state = False
                    logging.info(f"BMW is 404 - VIN: {vehicle['vin']}, Active: {current_state}")
                else:
                    error_msg = f"Failed to check vehicle status: HTTP {status_code}"
                    vehicle['activation_status'] = 'false'
                    await update_google_sheet_status(vehicle['vin'], None, error_msg)
                    return
                
                if current_state != desired_state:
                    if desired_state:
                        logging.info(f"Attempting to activate BMW vehicle - VIN: {vehicle['vin']}")
                        success, error_msg = await asyncio.wait_for(
                            self.activate_bmw(session, vehicle['vin']),
                            timeout=ACTIVATION_TIMEOUT
                        )
                    else:
                        logging.info(f"Attempting to deactivate BMW vehicle - VIN: {vehicle['vin']}")
                        success = await asyncio.wait_for(
                            self.deactivate_bmw(session, vehicle['vin']),
                            timeout=ACTIVATION_TIMEOUT
                        )
                        if not success:
                            error_msg = "Failed to deactivate BMW vehicle"
                        success = not success  # Invert success for deactivation
                    should_update_real_activation = True
                else:
                    logging.info(f"BMW vehicle {vehicle['vin']} already in desired state: {desired_state}")
                    success = desired_state
                    should_update_real_activation = True
                    
            elif make_lower in ['ford', 'mercedes', 'kia']:
                logging.info(f"Processing High Mobility vehicle - VIN: {vehicle['vin']}, Make: {make_lower}")
                # Always check current status first
                status_code, result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.hm_api.get_status(vehicle['vin'])
                )
                
                current_state = False
                if status_code == 200:
                    current_state = result.get('status') == "ACTIVE"
                    logging.info(f"High Mobility vehicle current state - VIN: {vehicle['vin']}, Active: {current_state}")
                elif status_code != 404:
                    error_msg = f"Failed to check vehicle status: HTTP {status_code}"
                    vehicle['activation_status'] = 'false'
                    await update_google_sheet_status(vehicle['vin'], None, error_msg)
                    return
                
                # Only take action if current state doesn't match desired state
                if current_state != desired_state:
                    if desired_state:
                        logging.info(f"Attempting to activate High Mobility vehicle - VIN: {vehicle['vin']}")
                        success, error_msg = await asyncio.wait_for(
                            self.activate_high_mobility(session, vehicle['vin'], make_lower),
                            timeout=ACTIVATION_TIMEOUT
                        )
                    else:
                        logging.info(f"Attempting to deactivate High Mobility vehicle - VIN: {vehicle['vin']}")
                        success = await asyncio.wait_for(
                            self.deactivate_high_mobility(session, vehicle['vin']),
                            timeout=ACTIVATION_TIMEOUT
                        )
                        if not success:
                            error_msg = "Failed to deactivate High Mobility vehicle"
                        success = not success  # Invert success for deactivation
                    should_update_real_activation = True
                else:
                    logging.info(f"High Mobility vehicle {vehicle['vin']} already in desired state: {desired_state}")
                    success = desired_state
                    should_update_real_activation = True
            elif make_lower in ['opel', 'citroën', 'ds', 'fiat', 'peugeot']:
                logging.info(f"Processing Stellantis vehicle - VIN: {vehicle['vin']}, Make: {make_lower}")
                # Always check current status first
                status_code, result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.stellantis_api.get_status(vehicle['vin'])
                )
                
            else:
                logging.info(f"Vehicle brand not supported for activation: {make_lower}")
                success = False
                error_msg = "Brand not supported for activation"
                should_update_real_activation = False
                
            vehicle['activation_status'] = str(success)            
            # Update Activation Error even if we don't update Real Activation
            if error_msg:
                await update_google_sheet_status(vehicle['vin'], None, error_msg)
            elif should_update_real_activation:
                # Only update Real Activation for actual activation/deactivation attempts
                await update_google_sheet_status(vehicle['vin'], success, None)
            else:
                logging.info(f"Skipping Real Activation update for VIN {vehicle['vin']} as no activation/deactivation was attempted")
            
        except asyncio.TimeoutError:
            error_msg = f"Timeout while processing {make_lower} vehicle activation"
            logging.error(error_msg)
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vehicle['vin'], None, error_msg)
            
        except Exception as e:
            error_msg = f"Error processing {make_lower} vehicle: {str(e)}"
            logging.error(error_msg)
            vehicle['activation_status'] = 'false'
            await update_google_sheet_status(vehicle['vin'], None, error_msg) 
