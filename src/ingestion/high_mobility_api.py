from datetime import datetime, timezone
from urllib.parse import quote, urlencode

import requests
from ingestion.vehicle import Vehicle


class HMApi:
    """
    Represents an instance of the HM API with a base URL (different in sandbox
    and prod), client id and client secret.
    """

    base_url = ""
    client_id = ""
    client_secret = ""

    __token = None
    __token_exp = float("inf")

    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.__fetch_token()

    def __fetch_token(self):
        r = requests.post(
            f"{self.base_url}/v1/access_tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        ).json()
        self.__token = r.get("access_token")
        timestamp = (
            datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        ).total_seconds()
        expires_in = int(r.get("expires_in"))
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
        self, status=None, brand=None
    ) -> tuple[int, list[Vehicle] | object]:
        """Lists clearances of vehicles activated through the HM API

        Arguments
        ---------
        status: string, optional
            The status filter
        brand: string, optional
            The brand filter

        Returns
        -------
        tuple[int, list[Vehicle] | object]
            A tuple containing the response's status code and the returned list of vehicles or object
        """
        filter = {}
        if status:
            filter["status"] = {"operator": "eq", "value": status}
        if brand:
            filter["brand"] = {"operator": "eq", "value": brand}
        if filter:
            encoded_filter = urlencode({"filter": filter}, quote_via=quote).replace(
                "%27", "%22"
            )
            token = self.__get_token()
            result = requests.get(
                f"{self.base_url}/v1/fleets/vehicles?{encoded_filter}",
                headers={"Authorization": f"Bearer {token}"},
            )
            if result.status_code == requests.codes.ok:
                return result.status_code, [
                    Vehicle(
                        vin=vehicle["vin"],
                        brand=vehicle["brand"],
                        clearance_status=vehicle["status"],
                    )
                    for vehicle in result.json()
                ]
            else:
                return result.status_code, result.json()
        token = self.__get_token()
        result = requests.get(
            f"{self.base_url}/v1/fleets/vehicles",
            headers={"Authorization": f"Bearer {token}"},
        )
        if result.status_code == requests.codes.ok:
            return result.status_code, [
                Vehicle(
                    vin=vehicle["vin"],
                    brand=vehicle["brand"],
                    clearance_status=vehicle["status"],
                )
                for vehicle in result.json()
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

    def get_vehicle_info(self, vin: str) -> tuple[int, object]:
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
            f"{self.base_url}/v1/vehicle-data/autoapi-13/{vin}",
            headers={"Authorization": f"Bearer {token}"},
        )
        return result.status_code, result.json()

