import logging
import requests
from typing import Tuple, Any, List, Dict

class BMWApi:
    """BMW API client for vehicle management."""
    
    def __init__(self, auth_url: str, base_url: str, client_id: str, fleet_id: str,
                 client_username: str, client_password: str):
        self.auth_url = auth_url
        self.base_url = base_url
        self.client_id = client_id
        self.fleet_id = fleet_id
        self.client_username = client_username
        self.client_password = client_password
        self._access_token = None

    def _get_auth_token(self) -> str:
        """Get authentication token from BMW API."""
        try:
            response = requests.post(
                self.auth_url,
                headers={
                    "Content-Type": "application/x-amz-json-1.1",
                    "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"
                },
                json={
                    "AuthParameters": {
                        "USERNAME": self.client_username,
                        "PASSWORD": self.client_password
                    },
                    "AuthFlow": "USER_PASSWORD_AUTH",
                    "ClientId": self.client_id
                }
            )
            response.raise_for_status()
            auth_result = response.json().get("AuthenticationResult", {})
            self._access_token = auth_result.get("IdToken")
            if not self._access_token:
                raise ValueError("No IdToken found in authentication response")
            return self._access_token
        except Exception as e:
            logging.error(f"Failed to get BMW auth token: {str(e)}")
            raise

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        if not self._access_token:
            self._get_auth_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    def check_vehicle_status(self, vin: str) -> Tuple[int, Any]:
        """Check vehicle status in BMW API."""
        return self.get_clearance(vin)

    def get_clearance(self, vin: str) -> Tuple[int, Any]:
        """Get vehicle clearance status."""
        try:
            url = f"{self.base_url}/vehicle/{vin}"
            response = requests.get(url, headers=self._get_headers())
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to get BMW clearance: {str(e)}")
            return 500, str(e)

    def create_clearance(self, vehicles: List) -> Tuple[int, Any]:
        """Create clearance for vehicles."""
        try:
            url = f"{self.base_url}/vehicle"
            response = requests.post(
                url,
                headers=self._get_headers(),
                json={"vehicles": vehicles}
            )
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to create BMW clearance: {str(e)}")
            return 500, str(e)

    def delete_clearance(self, vin: str) -> Tuple[int, Any]:
        """Delete vehicle clearance."""
        try:
            url = f"{self.base_url}/vehicle/{vin}"
            response = requests.delete(url, headers=self._get_headers())
            # For DELETE requests, often there is no content returned
            return response.status_code, response.text if response.text else ""
        except Exception as e:
            logging.error(f"Failed to delete BMW clearance: {str(e)}")
            return 500, str(e)

    def get_fleets(self) -> Tuple[int, Any]:
        """Get list of available fleets."""
        try:
            url = f"{self.base_url}/fleet"
            response = requests.get(url, headers=self._get_headers())
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to get BMW fleets: {str(e)}")
            return 500, str(e)

    def add_vehicle_to_fleet(self, fleet_id: str, vin: str) -> Tuple[int, Any]:
        """Add a vehicle to a fleet.
        
        Args:
            fleet_id: The ID of the fleet to add the vehicle to
            vin: The VIN of the vehicle to add
        """
        try:
            url = f"{self.base_url}/fleet/{fleet_id}/vehicle/{vin}"
            response = requests.post(url, headers=self._get_headers())
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to add vehicle to BMW fleet: {str(e)}")
            return 500, str(e) 
