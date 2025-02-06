import logging
import asyncio
import aiohttp
from typing import Tuple, Optional

from ..api.bmw_client import BMWApi
from ..api.hm_client import HMApi
from ..config.settings import ACTIVATION_TIMEOUT
from ..services.google_sheet_service import update_google_sheet_status

class VehicleActivationService:
    def __init__(self, bmw_api: BMWApi, hm_api: HMApi):
        self.bmw_api = bmw_api
        self.hm_api = hm_api

    async def activate_bmw(self, session: aiohttp.ClientSession, vin: str) -> Tuple[bool, Optional[str]]:
        """Activate a BMW vehicle using BMW's API."""
        try:
            # First check if vehicle is already activated
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.get_clearance(vin)
            )
            
            if status_code == 200:
                if result.clearance_status == "ACTIVE":
                    return True, None
                logging.info(f"BMW vehicle {vin} exists but not active, will reactivate")
            elif status_code != 404:
                return False, f"Failed to check vehicle status: HTTP {status_code}"
                
            # Create new clearance
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.create_clearance([{"vin": vin, "brand": "BMW"}])
            )
            
            if status_code in [200, 201, 204]:
                return True, None
            else:
                error_msg = f"Failed to activate BMW vehicle: HTTP {status_code}"
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
            # First check if vehicle is already activated
            status_code, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.hm_api.get_status(vin)
            )
            
            if status_code == 200:
                if result.get('status') == "ACTIVE":
                    return True, None
                logging.info(f"High Mobility vehicle {vin} exists but not active, will reactivate")
            elif status_code != 404:
                return False, f"Failed to check vehicle status: HTTP {status_code}"
                
            # Create new clearance with correct brand
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

    async def deactivate_bmw(self, session: aiohttp.ClientSession, vin: str) -> bool:
        """Deactivate a BMW vehicle."""
        try:
            logging.info(f"Starting BMW deactivation process for VIN: {vin}")
            
            # First check if vehicle exists
            status_code_first_check, result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.bmw_api.check_vehicle_status(vin)
            )
            
            if status_code_first_check == 404:
                logging.info(f"BMW vehicle {vin} already deactivated (404 Not Found)")
                return True
                
            if status_code_first_check != 200:
                logging.error(f"Failed to check BMW vehicle status: HTTP {status_code_first_check}")
                return False
                
            # Then delete if it exists
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
        is_activated = str(vehicle.get('activation', 'FALSE')).upper() == 'TRUE'
        real_activation = str(vehicle.get('real_activation', '')).upper()
        
        logging.info(f"Processing vehicle - VIN: {vehicle['vin']}, Make: {make_lower}, Activation: {is_activated}")
        
        # Only skip if real_activation is explicitly TRUE or FALSE
        if real_activation in ['TRUE', 'FALSE']:
            logging.info(f"Skipping API calls for VIN {vehicle['vin']} as Real Activation is already set to: {real_activation}")
            vehicle['activation_status'] = str(real_activation == 'TRUE')
            return
            
        try:
            success = False
            error_msg = None
            should_update_real_activation = False  # Flag to track if we should update Real Activation
            status_code = None  # Initialize status code variable
            
            if make_lower == 'bmw':
                logging.info(f"Processing BMW vehicle - VIN: {vehicle['vin']}")
                if is_activated:
                    logging.info(f"Attempting to activate BMW vehicle - VIN: {vehicle['vin']}")
                    success, error_msg = await asyncio.wait_for(
                        self.activate_bmw(session, vehicle['vin']),
                        timeout=ACTIVATION_TIMEOUT
                    )
                    logging.info(f"BMW activation result - VIN: {vehicle['vin']}, Success: {success}, Error: {error_msg or 'None'}")
                    should_update_real_activation = True
                else:
                    # Get initial status first
                    logging.info(f"Checking BMW vehicle status before deactivation - VIN: {vehicle['vin']}")
                    status_code, _ = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.bmw_api.check_vehicle_status(vehicle['vin'])
                    )
                    logging.info(f"BMW status check result - VIN: {vehicle['vin']}, Status Code: {status_code}")
                    
                    if status_code == 404:
                        logging.info(f"BMW vehicle already deactivated (404) - VIN: {vehicle['vin']}")
                        success = False  # Already deactivated, so we want Real Activation to be FALSE
                        should_update_real_activation = True
                    else:
                        logging.info(f"Attempting to deactivate BMW vehicle - VIN: {vehicle['vin']}")
                        success = await asyncio.wait_for(
                            self.deactivate_bmw(session, vehicle['vin']),
                            timeout=ACTIVATION_TIMEOUT
                        )
                        should_update_real_activation = True
                        if not success and status_code != 404:
                            error_msg = "Failed to deactivate BMW vehicle"
                        success = not success  # Invert success for deactivation
                        logging.info(f"BMW deactivation result - VIN: {vehicle['vin']}, Success: {success}, Error: {error_msg or 'None'}")
                    
            elif make_lower in ['ford', 'mercedes', 'kia']:
                logging.info(f"Processing High Mobility vehicle - VIN: {vehicle['vin']}, Make: {make_lower}")
                if is_activated:
                    success, error_msg = await asyncio.wait_for(
                        self.activate_high_mobility(session, vehicle['vin'], make_lower),
                        timeout=ACTIVATION_TIMEOUT
                    )
                    should_update_real_activation = True
                else:
                    # Get initial status first
                    status_code, _ = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.hm_api.get_status(vehicle['vin'])
                    )
                    
                    if status_code == 404:
                        success = False  # Already deactivated, so we want Real Activation to be FALSE
                        should_update_real_activation = True
                    else:
                        success = await asyncio.wait_for(
                            self.deactivate_high_mobility(session, vehicle['vin']),
                            timeout=ACTIVATION_TIMEOUT
                        )
                        should_update_real_activation = True
                        if not success and status_code != 404:
                            error_msg = "Failed to deactivate High Mobility vehicle"
                        success = not success  # Invert success for deactivation
            else:
                logging.info(f"Vehicle brand not supported for activation: {make_lower}")
                success = False
                error_msg = "Brand not supported for activation"
                should_update_real_activation = False
                
            vehicle['activation_status'] = str(success)
            logging.info(f"Final activation status - VIN: {vehicle['vin']}, Status: {success}, Should Update Real Activation: {should_update_real_activation}")
            
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
            # Update only the error message
            await update_google_sheet_status(vehicle['vin'], None, error_msg)
            
        except Exception as e:
            error_msg = f"Error processing {make_lower} vehicle: {str(e)}"
            logging.error(error_msg)
            vehicle['activation_status'] = 'false'
            # Update only the error message
            await update_google_sheet_status(vehicle['vin'], None, error_msg) 
