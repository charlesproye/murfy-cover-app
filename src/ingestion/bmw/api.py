from datetime import datetime, timezone
from urllib.parse import quote, urlencode
import json
import logging

import requests
from ingestion.bmw.vehicle import Vehicle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BMWApi:
    """
    Represents an instance of the BMW API with a base URL (different in sandbox
    and prod), client id and client secret.
    """

    base_url = ""
    client_id = ""
    client_username = ""
    client_password = ""

    __access_token = None
    __id_token = None
    __token_exp = float("inf")

    def __init__(self, auth_url: str, base_url: str, client_id: str, fleet_id: str, client_username: str, client_password: str):
        self.auth_url = auth_url
        self.base_url = base_url
        self.client_id = client_id
        self.fleet_id = fleet_id
        self.client_username = client_username
        self.client_password = client_password
        
        if not all([auth_url, base_url, client_id, fleet_id, client_username, client_password]):
            raise ValueError("Missing required BMW API credentials")
            
        self.__fetch_token()

    def __fetch_token(self):
        try:
            headers = {
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"
            }
            
            data = {
                "AuthParameters": {
                    "USERNAME": self.client_username,
                    "PASSWORD": self.client_password
                },
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": self.client_id
            }
            
            if not all([self.client_username, self.client_password, self.client_id]):
                raise ValueError("Missing required authentication credentials")
            
            r = requests.post(
                self.auth_url,
                headers=headers,
                data=json.dumps(data),
                timeout=10
            )
            
            r.raise_for_status()
            
            auth_result = r.json().get("AuthenticationResult")
            if not auth_result:
                raise Exception("No authentication result in response")
                
            # Store both AccessToken and IdToken
            self.__access_token = auth_result.get("AccessToken")
            self.__id_token = auth_result.get("IdToken")
            
            if not self.__id_token:
                raise Exception("No IdToken in authentication result")
                
            timestamp = (
                datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
            ).total_seconds()
            expires_in = int(auth_result.get("ExpiresIn", 3600))
            self.__token_exp = timestamp + expires_in
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Error status code: {e.response.status_code}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Failed to fetch token: {str(e)}")
            raise

    def __get_token(self, use_id_token=False):
        timestamp = (
            datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        ).total_seconds()
        if timestamp > self.__token_exp:
            self.__fetch_token()
        return self.__id_token if use_id_token else self.__access_token

    def create_clearance(self, vehicles: list[Vehicle]) -> tuple[int, object]:
        """Creates a clearance with the HM API

        Arguments
        ---------
        vehicles: string
            The vehicle list to activate (taken from `parse_vins`)

        Returns
        -------
        tuple[int, object]
            A tuple containing the response's status code and the returned object
        """
        token = self.__get_token()
        result = requests.post(
            f"{self.base_url}/v1/fleets/vehicles",
            json={
                "vehicles": [
                    {"vin": vehicle.vin, "brand": vehicle.brand, "tags": {}}
                    for vehicle in vehicles
                ]
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        return result.status_code, result.json()

    def list_clearances(self) -> tuple[int, list[Vehicle] | object]:
        """Lists clearances of vehicles activated through the HM API

        Returns
        -------
        tuple[int, list[Vehicle] | object]
            A tuple containing the response's status code and the returned list of vehicles or object
        """
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/fleet/{self.fleet_id}/vehicle",
            headers={"Authorization": f"Bearer {token}"},
        )
        if result.status_code == requests.codes.ok:
            vehicles_data = result.json().get("vehicles", [])
            return result.status_code, [
                Vehicle(
                    vin=vehicle["vin"],
                    brand="BMW",
                    clearance_status="ACTIVE" if vehicle["added_to_fleet"] else "INACTIVE",
                    licence_plate=vehicle.get("licence_plate"),
                    note=vehicle.get("note"),
                    contract_end_date=vehicle.get("contract", {}).get("end_date") if vehicle.get("contract") else None,
                    added_to_fleet=self.parse_date(vehicle["added_to_fleet"]),
                )
                for vehicle in vehicles_data
            ]
        else:
            return result.status_code, result.json()

    def parse_date(self, date_string):
        if date_string:
            try:
                return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z").date()
            except ValueError:
                return None
        return None

    def get_clearance(self, vin: str) -> tuple[int, Vehicle | object]:
        """Get the clearance for a single vehicle"""
        try:
            token = self.__get_token(use_id_token=True)
            headers = {
                "Authorization": f"IdToken {token}",
                "Content-Type": "application/json"
            }
            
            result = requests.get(
                f"{self.base_url}/vehicle/{vin}",
                headers=headers
            )
            
            if result.status_code == requests.codes.ok:
                res = result.json()
                return result.status_code, Vehicle(
                    vin=res["vin"],
                    brand=res["brand"],
                    clearance_status=res["status"],
                )
            else:
                if result.text:
                    try:
                        return result.status_code, result.json()
                    except:
                        return result.status_code, result.text
                return result.status_code, None
        except Exception as e:
            logging.error(f"Error in get_clearance: {str(e)}")
            raise

    def delete_clearance(self, vin: str) -> tuple[int, Vehicle | object]:
        """Delete the clearance for a single vehicle"""
        try:
            id_token = self.__get_token(use_id_token=True)
            if not id_token:
                raise Exception("Failed to get ID token")
            
            headers = {
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json"
            }
            
            result = requests.delete(
                f"{self.base_url}/vehicle/{vin}",
                headers=headers
            )
            
            if result.status_code == 204:
                return result.status_code, None
            else:
                if result.text:
                    try:
                        error_json = result.json()
                        return result.status_code, error_json
                    except:
                        return result.status_code, result.text
                return result.status_code, None
        except Exception as e:
            logging.error(f"Error in delete_clearance: {str(e)}")
            raise

    def get_vehicle_info(self, vin: str) -> tuple[int, bytes]:
        """Get a vehicle's info

        Arguments
        ---------
        vin: str
            The VIN of the vehicle

        Returns
        -------
        tuple[int, object]
            A tuple containing the response's status code and the returned object
        """
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/data/vehicle/{vin}",
            headers={"Authorization": f"Bearer {token}", "keys-list": "battery_voltage,charging_ac_ampere,charging_ac_voltage,soc_target_charging_time_forecast,basic_charging_modes,last_trip_electric_energy_consumption_comfort,avg_electric_range_consumption,steering,eco_drive_mode_mileage_yesterday,weighted_avg_energy_consumption_yesterday,electric_share_yesterday,soc_hv_header,model,charging_planning_mode,charging_plug_connected_dc,charging_plug_connected,charging_status,charging_method,charge_plug_event_id,charge_plug_locked,soc_customer_target_range,soc_customer_target,brand,last_trip_ratio_electric_driven_distance,basic_construction_date,last_trip_SOC_segment_end,drive_in_inspection_due_date,power,last_trip_electric_recuperation_overall,speed_avg,electric_mileage_yesterday,travelled_distance_yesterday,remaining_mileage,capacity,mileage,data_quality,basic_model_range,teleservice_status,kombi_remaining_electric_range,basic_propulsion_type,coolant_temperature,average_mileage_per_week,basic_drive_train"},
        )
        return result.status_code, result.content

    def __force_refresh_token(self):
        """Force a token refresh regardless of expiration"""
        self.__fetch_token()
        return self.__id_token if self.__id_token else None

    def check_vehicle_status(self, vin: str) -> tuple[int, Vehicle | object]:
        """Check a vehicle's status using the direct vehicle endpoint

        Arguments
        ---------
        vin: str
            The VIN of the vehicle to check

        Returns
        -------
        tuple[int, Vehicle | object]
            A tuple containing the response's status code and the vehicle object or error
        """
        try:
            id_token = self.__get_token(use_id_token=True)
            if not id_token:
                raise Exception("Failed to get ID token")
            
            headers = {
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json"
            }
            
            result = requests.get(
                f"{self.base_url}/vehicle/{vin}",
                headers=headers
            )
            
            if result.status_code == requests.codes.ok:
                res = result.json()
                return result.status_code, {
                    "vin": vin,
                    "fleet": self.fleet_id,
                    "licence_plate": res.get("licence_plate", ""),
                    "note": res.get("note", ""),
                    "contract": res.get("contract"),
                    "created": res.get("created"),
                    "updated": res.get("updated"),
                    "added_to_fleet": res.get("added_to_fleet")
                }
            else:
                if result.text:
                    try:
                        error_json = result.json()
                        return result.status_code, error_json
                    except:
                        return result.status_code, result.text
                return result.status_code, None
        except Exception as e:
            logging.error(f"Error checking vehicle status: {str(e)}")
            raise

