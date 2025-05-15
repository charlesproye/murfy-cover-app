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
from ingestion.vehicle_info.api.renault_client import RenaultApi
from ingestion.vehicle_info.api.tesla_particulier import TeslaParticulierApi
from ingestion.vehicle_info.config.settings import ACTIVATION_TIMEOUT
from ingestion.vehicle_info.services.google_sheet_service import update_vehicle_activation_data

class VehicleActivationService:

    def __init__(self, bmw_api: BMWApi, hm_api: HMApi, stellantis_api: StellantisApi, tesla_api: TeslaApi, tesla_particulier_api: TeslaParticulierApi, renault_api: RenaultApi, fleet_info_df: pd.DataFrame):
        self.bmw_api = bmw_api
        self.hm_api = hm_api
        self.stellantis_api = stellantis_api
        self.tesla_api = tesla_api
        self.tesla_particulier_api = tesla_particulier_api
        self.renault_api = renault_api
        self.fleet_info_df = fleet_info_df
        
    async def _add_to_fleet(self, vin: str, session: aiohttp.ClientSession) -> Tuple[bool, Optional[str]]:
        """Add a vehicle to the appropriate fleet based on ownership.
        
        Args:
            vin: Vehicle VIN
        """
        try:
            vehicle_info = self.fleet_info_df[self.fleet_info_df['vin'] == vin]
            target_fleet_name = str(vehicle_info['owner'].iloc[0]) if 'owner' in vehicle_info.columns else 'bib'
            
            status_code, result = await self.bmw_api.get_fleets(session)
            
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
            
            status_code, result = await self.bmw_api.add_vehicle_to_fleet(target_fleet_id, vin,session)
            
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
            
            status_code, result = await self.bmw_api.create_clearance(payload,session)
            
            logging.info(f"Create clearance response")
            
            if status_code in [200, 201, 204]:
                fleet_success, fleet_error = await self._add_to_fleet(vin,session)
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
        
    async def activation_stellantis(self):
        """Process Stellantis vehicle activation/deactivation."""
        df_stellantis = self.fleet_info_df[(self.fleet_info_df['oem'] == 'stellantis')]
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_stellantis.iterrows():
                vin = row['vin']
                desired_state = row['activation']
                is_eligible = await self.stellantis_api.is_eligible(vin,session)
                
                if not is_eligible:
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': False,
                        'Real_Activation': False,
                        'Activation_Error': 'Not eligible'
                    }
                    if desired_state:
                        logging.info(f"Stellantis vehicle {vin} should be activated but is not eligible")
                    else:
                        logging.info(f"Stellantis vehicle {vin} is already in desired state: {desired_state}")
                    status_data.append(vehicle_data)
                    continue
            
                current_state, contract_id = await self.stellantis_api.get_status(vin,session)

                if current_state == desired_state:
                    logging.info(f"Stellantis vehicle {vin} is already in desired state: {desired_state}")
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': current_state,
                        'Real_Activation': current_state,
                        'Activation_Error': None
                    }
                    status_data.append(vehicle_data)
                    continue
                    
                elif not desired_state:
                    if contract_id:
                        status_code, error_msg = await self.stellantis_api.deactivate(contract_id,session)
                        real_state, contract_id = await self.stellantis_api.get_status(vin,session)
                        if status_code in [200, 204]:
                            logging.info(f"Stellantis vehicle {vin} deactivated successfully")
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': real_state,
                                'Real_Activation': False,
                                'Activation_Error': None
                            }
                            status_data.append(vehicle_data)
                            continue
                        else:
                            logging.info(f"Failed to deactivate Stellantis vehicle {vin}: HTTP {status_code} - {error_msg}")
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': real_state,
                                'Real_Activation': False,
                                'Activation_Error': "Failed to deactivate"
                            }
                            status_data.append(vehicle_data)
                            continue
                    else :
                        logging.info(f"Failed to deactivate Stellantis vehicle {vin} has no contract ID")
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': real_state,
                            'Real_Activation': False,
                            'Activation_Error': "No contract ID found"
                        }
                        status_data.append(vehicle_data)
                        continue

                elif desired_state:
                    status_code, result = await self.stellantis_api.activate(vin, session)
                    real_state,contract_id = await self.stellantis_api.get_status(vin,session)
                    if status_code in [200, 201, 204]:
                        logging.info(f"Stellantis vehicle {vin} activated successfully")
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': real_state,
                            'Real_Activation': real_state,
                            'Activation_Error': None
                        }
                        status_data.append(vehicle_data)
                        continue
                    elif status_code == 409:
                        logging.info(f"Stellantis vehicle {vin} activation in progress")
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': real_state,
                            'Real_Activation': real_state,
                            'Activation_Error': 'Activation in progress'
                        }
                        status_data.append(vehicle_data)
                        continue
                    else:
                        error_msg = f"Failed to activate Stellantis vehicle {vin}: HTTP {status_code} - {result}"
                        logging.error(error_msg)
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': real_state,
                            'Real_Activation': real_state,
                            'Activation_Error': error_msg
                        }
                        status_data.append(vehicle_data)
                        continue

        status_df = pd.DataFrame(status_data)
        await update_vehicle_activation_data(status_df)

    async def activation_tesla(self):
        """Process Tesla vehicle activation/deactivation.
        
        Returns:
            pd.DataFrame: DataFrame containing vehicle status with columns:
                - vin: Vehicle identification number
                - Eligibility: Whether the vehicle is eligible for activation
                - Real_Activation: Current activation status
                - Activation_Error: Any error messages
                - account_owner: Tesla account owner name
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
                    'Real_Activation': ggsheet_tesla[ggsheet_tesla['vin'] == vin]['activation'].iloc[0] == True if not ggsheet_tesla[ggsheet_tesla['vin'] == vin].empty else False,
                    'Activation_Error': None,
                    'account_owner': account_name
                }
                for vin, account_name in api_tesla
            ])
            print(f"Tesla vehicles in API: {len(api_vehicles)}")
            
            # Create DataFrame for vehicles not in API
            missing_vehicles = pd.DataFrame([
                {
                    'vin': vin,
                    'Eligibility': False,
                    'Real_Activation': False,
                    'Activation_Error': 'Vehicle not found in Tesla accounts',
                    'account_owner': None
                }
                for vin in ggsheet_tesla['vin']
                if vin not in [v[0] for v in api_tesla]  # Extract VINs from tuples for comparison
            ])
            print(f"Missing tesla vehicles): {len(missing_vehicles)}")
            
            # Combine both DataFrames
            status_df = pd.concat([api_vehicles, missing_vehicles], ignore_index=True)
            print(f"Total tesla vehicles: {len(status_df)}")
            await update_vehicle_activation_data(status_df)
        
    async def activation_bmw(self):
        """Process BMW vehicle activation/deactivation."""
        df_bmw = self.fleet_info_df[self.fleet_info_df['oem'] == 'bmw']
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_bmw.iterrows():
                vin = row['vin']
                desired_state = row['activation']
                current_state = await self.bmw_api.check_vehicle_status(vin,session)

                if desired_state == current_state:
                    logging.info(f"BMW vehicle {vin} already in desired state: {desired_state}")
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': current_state,
                        'Real_Activation': current_state,
                        'Activation_Error': None
                    }
                    status_data.append(vehicle_data)
                    continue

                elif desired_state:
                    success, _ = await self._activate_bmw(session, vin)
                    if success:
                        logging.info(f"BMW vehicle {vin} activated successfully")
                        vehicle_data = {
                        'vin': vin,
                        'Eligibility': True,
                        'Real_Activation': True,
                        'Activation_Error': None
                        }
                        status_data.append(vehicle_data)
                        continue
                    else:
                        logging.info(f"BMW vehicle {vin} activation failed")
                        vehicle_data = {
                        'vin': vin,
                            'Eligibility': False,
                            'Real_Activation': False,
                            'Activation_Error': 'Activation failed'
                        }
                        status_data.append(vehicle_data)
                        continue

                else:
                    try:
                        deactivation_success = await self.bmw_api.deactivate(vin,session)
                        
                        if deactivation_success:
                            current_state = await self.bmw_api.check_vehicle_status(vin,session)
                            
                            if current_state == False:
                                logging.info(f"BMW vehicle {vin} deactivated successfully")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': False,
                                    'Real_Activation': False,
                                    'Activation_Error': None
                                }
                                status_data.append(vehicle_data)
                                continue
                            else:
                                logging.info(f"BMW vehicle {vin} deactivation seemed successful but vehicle is still active")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': True,
                                    'Real_Activation': False,
                                    'Activation_Error': 'Deactivation seemed successful but vehicle is still active'
                                }
                                status_data.append(vehicle_data)
                                continue
                        else:
                            logging.info(f"BMW vehicle {vin} deactivation failed - API returned failure")
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': 'Failed to deactivate BMW vehicle - API returned failure'
                            }
                            status_data.append(vehicle_data)
                            continue
                    except Exception as e:
                        logging.info(f"BMW vehicle {vin} deactivation failed - Error: {str(e)}")
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': True,
                            'Real_Activation': False,
                            'Activation_Error': 'Failed to deactivate BMW vehicle'
                        }
                        status_data.append(vehicle_data)
                        continue
                    
            status_df = pd.DataFrame(status_data)
            await update_vehicle_activation_data(status_df)

    async def activation_hm(self):
        """Process High Mobility vehicle activation/deactivation."""
        df_hm = self.fleet_info_df[self.fleet_info_df['oem'].isin(['ford', 'mercedes', 'kia','renault'])]
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_hm.iterrows():
                vin = row['vin']
                make = row['make']
                make_lower = make.lower()

                desired_state = row['activation']
                
                try:
                    current_state = await self.hm_api.get_status(vin,session)
                    if current_state == desired_state:
                        logging.info(f"High Mobility vehicle {vin} already in desired state: {desired_state}")
                        vehicle_data = {
                            'vin': vin,
                            'Eligibility': current_state,
                            'Real_Activation': current_state,
                            'Activation_Error': None
                        }
                        status_data.append(vehicle_data)
                        continue
                        
                    elif desired_state:
                        try:
                            activation_success = await self.hm_api.create_clearance(vin,make,session)

                            if activation_success:
                                logging.info(f"High Mobility vehicle {vin} activated successfully")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': True,
                                    'Real_Activation': True,
                                    'Activation_Error': None
                                }
                                status_data.append(vehicle_data)
                                continue
                            else:
                                logging.info(f"High Mobility vehicle {vin} activation failed")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': False,
                                    'Real_Activation': False,
                                    'Activation_Error': 'Failed to activate or pending approval'
                                }
                                status_data.append(vehicle_data)
                                continue
                        except Exception as e:
                            logging.error(f"Error activating High Mobility vehicle: {str(e)}")
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': 'Failed to activate or pending approval'
                            }
                            status_data.append(vehicle_data)
                            continue
                    else:
                        try:
                            deactivation_success = await self.hm_api.delete_clearance(vin,session)
                            if not deactivation_success:
                                logging.error(f"Failed to deactivate High Mobility vehicle")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': True,
                                    'Real_Activation': False,
                                    'Activation_Error': 'Failed to deactivate or pending approval'
                                }
                                status_data.append(vehicle_data)
                                continue
                            else:
                                logging.info(f"High Mobility vehicle {vin} deactivated successfully")
                                vehicle_data = {
                                    'vin': vin,
                                    'Eligibility': False,
                                    'Real_Activation': False,
                                    'Activation_Error': None
                                }
                                status_data.append(vehicle_data)
                                continue
                        except Exception as e:
                            logging.error(f"Error deactivating High Mobility vehicle: {str(e)}")
                            vehicle_data = {
                                'vin': vin,
                                'Eligibility': True,
                                'Real_Activation': False,
                                'Activation_Error': 'Failed to deactivate or pending approval'
                            }
                            status_data.append(vehicle_data)
                            continue
                            
                except Exception as e:
                    logging.error(f"Error processing High Mobility vehicle: {str(e)}")
                    vehicle_data = {
                        'vin': vin,
                        'Eligibility': True,
                        'Real_Activation': False,
                        'Activation_Error': 'Failed to process High Mobility vehicle'
                    }
                    status_data.append(vehicle_data)
                    continue
        status_df = pd.DataFrame(status_data)
        await update_vehicle_activation_data(status_df)
    
