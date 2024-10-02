from datetime import datetime, timezone
from urllib.parse import quote, urlencode
import json

import requests
from ingestion.bmw.vehicle import Vehicle


class BMWApi:
    """
    Represents an instance of the BMW API with a base URL (different in sandbox
    and prod), client id and client secret.
    """

    base_url = ""
    client_id = ""
    client_username = ""
    client_password = ""

    __token = None
    __token_exp = float("inf")

    def __init__(self, auth_url: str, base_url: str, client_id: str, fleet_id: str, client_username: str, client_password: str):
        self.auth_url = auth_url
        self.base_url = base_url
        self.client_id = client_id
        self.fleet_id = fleet_id
        self.client_username = client_username
        self.client_password = client_password
        self.__fetch_token()

    def __fetch_token(self):
        r = requests.post(
            f"{self.auth_url}",
            data=json.dumps({
                "AuthParameters": {
                    "USERNAME": self.client_username,
                    "PASSWORD": self.client_password
                },
                "AuthFlow": "USER_PASSWORD_AUTH",
                "ClientId": self.client_id
            }),
            headers={"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"}
        )
        self.__token = r.json().get("AuthenticationResult").get("IdToken")
        timestamp = (
            datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        ).total_seconds()
        expires_in = int(r.json().get("AuthenticationResult").get("ExpiresIn"))
        self.__token_exp = timestamp + expires_in

    def __get_token(self):
        timestamp = (
            datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        ).total_seconds()
        if timestamp > self.__token_exp:
            self.__fetch_token()
        return self.__token

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

    def list_clearances(
        self,
    ) -> tuple[int, list[Vehicle] | object]:
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
                    contract_end_date=vehicle["contract"]["end_date"],
                    added_to_fleet=vehicle["added_to_fleet"],
                )
                for vehicle in vehicles_data
            ]
        else:
            return result.status_code, result.json()

    def get_clearance(self, vin: str) -> tuple[int, Vehicle | object]:
        """Get the clearance for a single vehicle

        Arguments
        ---------
        vin: str
            The VIN of the vehicle

        Returns
        -------
        tuple[int, Vehicle | object]
            A tuple containing the response's status code and the returned vehicle or object
        """
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/v1/fleets/vehicles/{vin}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if result.status_code == requests.codes.ok:
            res = result.json()
            return result.status_code, Vehicle(
                vin=res["vin"],
                brand=res["brand"],
                clearance_status=res["status"],
            )
        else:
            return result.status_code, result.json()

    def delete_clearance(self, vin: str) -> tuple[int, Vehicle | object]:
        """Delete the clearance for a single vehicle

        Arguments
        ---------
        vin: str
            The VIN of the vehicle

        Returns
        -------
        tuple[int, Vehicle | object]
            A tuple containing the response's status code and the returned vehicle or object
        """
        token = self.__get_token()
        result = requests.delete(
            f"{self.base_url}/v1/fleets/vehicles/{vin}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if result.status_code == requests.codes.ok:
            res = result.json()
            return result.status_code, Vehicle(
                vin=res["vin"],
                brand=res["brand"],
                clearance_status=res["status"],
            )
        else:
            return result.status_code, result.json()

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

