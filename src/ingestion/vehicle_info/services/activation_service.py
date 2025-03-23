import logging
import asyncio
import aiohttp
from typing import Tuple, Optional
import pandas as pd
import requests
import uuid

from ingestion.vehicle_info.api.bmw_client import BMWApi
from ingestion.vehicle_info.api.hm_client import HMApi
from ingestion.vehicle_info.api.stellantis_client import StellantisApi
from ingestion.vehicle_info.api.tesla_client import TeslaApi
from ingestion.vehicle_info.config.settings import ACTIVATION_TIMEOUT
from ingestion.vehicle_info.services.google_sheet_service import update_vehicle_activation_data

class VehicleActivationService:

    def __init__(self, bmw_api: BMWApi, hm_api: HMApi, stellantis_api: StellantisApi, tesla_api: TeslaApi, fleet_info_df: pd.DataFrame):
        self.bmw_api = bmw_api
        self.hm_api = hm_api
        self.stellantis_api = stellantis_api
        self.tesla_api = tesla_api
        self.fleet_info_df = fleet_info_df

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

    async def _activate_bmw(self, session: aiohttp.ClientSession, vin: str) -> Tuple[bool, Optional[str]]:
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
        
    async def _deactivate_bmw(self, session: aiohttp.ClientSession, vin: str) -> bool:
        """Deactivate a BMW vehicle."""
        try:            
            status_code = await self.bmw_api.delete_clearance(vin)
            
            success = status_code in [200, 204]
            if not success:
                logging.error(f"Failed to deactivate BMW vehicle {vin}: HTTP {status_code}")
            return success
            
        except Exception as e:
            logging.error(f"Error deactivating BMW vehicle {vin}: {str(e)}")
            return False

    async def _activate_high_mobility(self, session: aiohttp.ClientSession, vin: str, make: str) -> Tuple[bool, Optional[str]]:
        """Activate a vehicle using High Mobility's API.
        
        Args:
            session: aiohttp client session
            vin: Vehicle VIN
            make: Vehicle make/brand
        """
        try:
            status_code = await self.hm_api.create_clearance([{"vin": vin, "brand": make}])

            if status_code in [200, 201, 204]:
                return True, None
            else:
                error_msg = f"Failed to activate High Mobility vehicle: HTTP {status_code}"
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error activating High Mobility vehicle: {str(e)}"
            logging.error(error_msg)
            return False, error_msg
        
    async def _deactivate_high_mobility(self, session: aiohttp.ClientSession, vin: str) -> bool:
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
                
            status_code = await self.hm_api.delete_clearance(vin)
            success = status_code in [200, 204]
            return success
            
        except Exception as e:
            logging.error(f"Error deactivating High Mobility vehicle {vin}: {str(e)}")
            return False

    async def activation_stellantis(self) -> pd.DataFrame:
        """Process Stellantis vehicle activation/deactivation."""
        df_stellantis = self.fleet_info_df[self.fleet_info_df['oem'] == 'stellantis']
        status_data = []
        async with aiohttp.ClientSession() as session:
            for index, row in df_stellantis.iterrows():
                vin = row['vin']
                logging.info(f"Processing Stellantis vehicle - VIN: {vin}")
                desired_state = row['activation']
                is_eligible = self.stellantis_api.is_eligible(vin)
                
                if not is_eligible:
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': False,
                        'Real_Activation': False,
                        'Activation_Error': 'Vehicle not eligible'
                    }
                    status_data.append(vehicle_data)
                    continue
            
                status_code, result = self.stellantis_api.get_status(vin)
            
                contract_id = None
                if status_code == 200 and result:
                    contract_id = result.get("_id")
                status = result.get("status", "")
                current_state = status == "activated"
                
                if current_state == desired_state:
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': current_state,
                        'Real_Activation': current_state,
                        'Activation_Error': None
                    }
                    status_data.append(vehicle_data)
                    continue
                    
                    
                if not desired_state:
                    logging.info(f"Vehicle is activated but deactivation is requested - VIN: {vin}")
                    if contract_id:
                        status_code = self.stellantis_api.delete_clearance(contract_id)
                        if status_code in [200, 204]:
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': False,
                                'Real_Activation': False,
                                'Activation_Error': None
                            }
                            status_data.append(vehicle_data)
                            continue
                        else:
                            error_msg = f"Failed to deactivate Stellantis vehicle: HTTP {status_code}"
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': error_msg
                            }
                            status_data.append(vehicle_data)
                            continue
                    else :
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': True,
                            'Real_Activation': False,
                            'Activation_Error': "No contract ID found"
                        }
                        status_data.append(vehicle_data)
                        continue

                if desired_state:
                    status_code = self.stellantis_api.create_clearance([{"vin": vin}])
                    if status_code in [200, 201, 204]:
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': True,
                            'Real_Activation': True,
                            'Activation_Error': None
                        }
                    elif status_code == 409:
                        error_msg = "Vehicle already has an active contract or activation in progress"
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': True,
                            'Real_Activation': True,
                            'Activation_Error': error_msg
                        }
                        status_data.append(vehicle_data)
                        continue
                    else:
                        error_msg = f"Failed to activate Stellantis vehicle: HTTP {status_code}"
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': False,
                            'Real_Activation': False,
                            'Activation_Error': error_msg
                        }
                        status_data.append(vehicle_data)
                        continue

        status_df = pd.DataFrame(status_data)
        await update_vehicle_activation_data(status_df)

    async def activation_tesla(self) -> pd.DataFrame:
        """Process Tesla vehicle activation/deactivation.
        
        Returns:
            pd.DataFrame: DataFrame containing vehicle status with columns:
                - vin: Vehicle identification number
                - Eligibility: Whether the vehicle is eligible for activation
                - Real_Activation: Current activation status
                - Activation_Error: Any error messages
        """
        logging.info("Checking eligibility of Tesla vehicles")
        
        # Get Tesla vehicles from fleet info
        ggsheet_tesla = self.fleet_info_df[self.fleet_info_df['oem'] == 'tesla']
        
        # Get Tesla API data
        async with aiohttp.ClientSession() as session:
            api_tesla = await self.tesla_api._build_vin_mapping(session)
            
            # Create DataFrame for vehicles in API
            api_vehicles = pd.DataFrame([
                {
                    'vin': vin,
                    'Eligibility': True,
                    'Real_Activation': ggsheet_tesla[ggsheet_tesla['vin'] == vin]['activation'].iloc[0] == 'True' if not ggsheet_tesla[ggsheet_tesla['vin'] == vin].empty else False,
                    'Activation_Error': None
                }
                for vin in api_tesla
            ])
            
            # Create DataFrame for vehicles not in API
            missing_vehicles = pd.DataFrame([
                {
                    'vin': vin,
                    'Eligibility': False,
                    'Real_Activation': False,
                    'Activation_Error': 'Vehicle not found in Tesla accounts'
                }
                for vin in ggsheet_tesla['vin']
                if vin not in api_tesla
            ])
            
            # Combine both DataFrames
            status_df = pd.concat([api_vehicles, missing_vehicles], ignore_index=True)
            await update_vehicle_activation_data(status_df)
        
    async def activation_bmw(self) -> pd.DataFrame:
        """Process BMW vehicle activation/deactivation."""
        df_bmw = self.fleet_info_df[self.fleet_info_df['oem'] == 'bmw']
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_bmw.iterrows():
                vin = row['vin']
                logging.info(f"Processing BMW vehicle - VIN: {vin}")
                desired_state = row['activation'] == 'True'
                current_state = self.bmw_api.check_vehicle_status(vin)
                if desired_state == current_state:
                    logging.info(f"BMW vehicle {vin} already in desired state: {desired_state}")
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': current_state,
                        'Real_Activation': current_state,
                        'Activation_Error': None
                    }
                    status_data.append(vehicle_data)
                
                elif desired_state:
                    logging.info(f"Attempting to activate BMW vehicle - VIN: {vin}")
                    success, _ = await asyncio.wait_for(
                        self._activate_bmw(session, vin),
                        timeout=ACTIVATION_TIMEOUT
                    )
                    if success:
                        logging.info(f"BMW vehicle {vin} activated successfully")
                        vehicle_data = {
                        'vin': vin,
                        'Eligibility': True,
                        'Real_Activation': True,
                        'Activation_Error': None
                        }
                        status_data.append(vehicle_data)
                    else:
                        logging.info(f"BMW vehicle {vin} activation failed")
                        vehicle_data = {
                        'vin': vin,
                        'Eligibility': False,
                        'Real_Activation': False,
                        'Activation_Error': 'Activation failed'
                        }
                        status_data.append(vehicle_data)

                else:
                    logging.info(f"Attempting to deactivate BMW vehicle - VIN: {vin}")
                    try:
                        deactivation_success = await asyncio.wait_for(
                            self._deactivate_bmw(session, vin),
                            timeout=ACTIVATION_TIMEOUT
                        )
                        
                        if deactivation_success:
                            current_state = self.bmw_api.check_vehicle_status(vin)
                            
                            if current_state == False:
                                logging.info(f"BMW vehicle {vin} deactivated successfully")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': False,
                                    'Real_Activation': False,
                                    'Activation_Error': None
                                }
                                status_data.append(vehicle_data)
                            else:
                                error_msg = f"Deactivation seemed successful but vehicle is still active"
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': True,
                                    'Real_Activation': False,
                                    'Activation_Error': 'Deactivation seemed successful but vehicle is still active'
                                }
                                status_data.append(vehicle_data)
                        else:
                            error_msg = "Failed to deactivate BMW vehicle - API returned failure"
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': 'Failed to deactivate BMW vehicle - API returned failure'
                            }
                            status_data.append(vehicle_data)

                    except Exception as e:
                        error_msg = f"Failed to deactivate BMW vehicle - Error: {str(e)}"
                        logging.error(error_msg)
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': True,
                            'Real_Activation': False,
                            'Activation_Error': 'Failed to deactivate BMW vehicle - Error'
                        }
                        status_data.append(vehicle_data)
                    
            status_df = pd.DataFrame(status_data)
            await update_vehicle_activation_data(status_df)

    async def activation_hm(self) -> pd.DataFrame:
        """Process High Mobility vehicle activation/deactivation."""
        df_hm = self.fleet_info_df[self.fleet_info_df['oem'].isin(['ford', 'mercedes', 'kia','renault'])]
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_hm.iterrows():
                vin = row['vin']
                logging.info(f"Processing High Mobility vehicle - VIN: {vin}")
                make = row['make']
                make_lower = make.lower()
                desired_state = row['activation']
                
                try:
                    current_state = self.hm_api.get_status(vin)
                        
                    if current_state == desired_state:
                        logging.info(f"High Mobility vehicle {vin} already in desired state: {desired_state}")
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': current_state,
                            'Real_Activation': current_state,
                            'Activation_Error': None
                        }
                        status_data.append(vehicle_data)
                        
                    elif desired_state:
                        logging.info(f"Attempting to activate High Mobility vehicle - VIN: {vin}")
                        try:
                            success, error_msg = await asyncio.wait_for(
                                self._activate_high_mobility(session, vin, make_lower),
                                timeout=ACTIVATION_TIMEOUT
                            )
                            if success:
                                logging.info(f"High Mobility vehicle {vin} activated successfully")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': True,
                                    'Real_Activation': True,
                                    'Activation_Error': None
                                }
                                status_data.append(vehicle_data)
                            else:
                                logging.info(f"High Mobility vehicle {vin} activation failed")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': False,
                                    'Real_Activation': False,
                                    'Activation_Error': error_msg
                                }
                                status_data.append(vehicle_data)
                        except Exception as e:
                            error_msg = f"Error activating High Mobility vehicle: {str(e)}"
                            logging.error(error_msg)
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': error_msg
                            }
                            status_data.append(vehicle_data)
                    else:
                        logging.info(f"Attempting to deactivate High Mobility vehicle - VIN: {vin}")
                        try:
                            success = await asyncio.wait_for(
                                self._deactivate_high_mobility(session, vin),
                                timeout=ACTIVATION_TIMEOUT
                            )
                            if not success:
                                error_msg = "Failed to deactivate High Mobility vehicle"
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': True,
                                    'Real_Activation': False,
                                    'Activation_Error': error_msg
                                }
                                status_data.append(vehicle_data)
                            else:
                                logging.info(f"High Mobility vehicle {vin} deactivated successfully")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': False,
                                    'Real_Activation': False,
                                    'Activation_Error': None
                                }
                                status_data.append(vehicle_data)

                        except Exception as e:
                            error_msg = f"Error deactivating High Mobility vehicle: {str(e)}"
                            logging.error(error_msg)
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': error_msg
                            }
                            status_data.append(vehicle_data)
                            
                except Exception as e:
                    error_msg = f"Error processing High Mobility vehicle: {str(e)}"
                    logging.error(error_msg)
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': True,
                        'Real_Activation': False,
                        'Activation_Error': error_msg
                    }
                    status_data.append(vehicle_data)

        status_df = pd.DataFrame(status_data)
        await update_vehicle_activation_data(status_df)
    
