import logging
import requests
from datetime import datetime, timezone
from typing import Tuple, Any, List, Dict

class StellantisApi:
    """Stellantis API client for vehicle management."""
    
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = None

    def _get_auth_token(self) -> str:
        """Get authentication token from High Mobility API."""
        try:
            response = requests.post(
                f"{self.base_url}/v1/access_tokens",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=10
            )
            response.raise_for_status()
            response_data = response.json()
            self._access_token = response_data.get("access_token")
            timestamp = (
                datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
            ).total_seconds()
            expires_in = int(response_data.get("expires_in", 3600))  # Default 1 hour if not specified
            self.__token_exp = timestamp + expires_in
            return self._access_token
        except Exception as e:
            logging.error(f"Failed to get Stellantis auth token: {str(e)}")
            raise

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        if not self._access_token:
            self._get_auth_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    def get_status(self, vin: str) -> Tuple[int, Any]:
        """Get vehicle status."""
        try:
            url = f"{self.base_url}/v1/fleets/vehicles/{vin}"
            response = requests.get(url, headers=self._get_headers())
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to get Stellantis vehicle status: {str(e)}")
            return 500, str(e)

    def get_clearance(self, vin: str) -> Tuple[int, Any]:
        """Get vehicle clearance status."""
        try:
            url = f"{self.base_url}/vehicles/{vin}/clearance"
            response = requests.get(url, headers=self._get_headers())
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to get Stellantis clearance: {str(e)}")
            return 500, str(e)

    def create_clearance(self, vehicles: List[Dict[str, str]]) -> Tuple[int, Any]:
        """Create clearance for vehicles.
        
        Args:
            vehicles: List of dicts containing 'vin' and 'brand' for each vehicle
        """
        try:
            url = f"{self.base_url}/v1/vehicle"
            # Ensure each vehicle has required fields in correct format
            formatted_vehicles = [
                {
                    "vin": vehicle["vin"],
                    "brand": vehicle["brand"].lower(),
                }
                for vehicle in vehicles
            ]
            
            response = requests.post(
                url,
                headers=self._get_headers(),
                json={"vehicles": formatted_vehicles}
            )
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to create Stellantis clearance: {str(e)}")
            return 500, str(e)

    def delete_clearance(self, vin: str) -> Tuple[int, Any]:
        """Delete vehicle clearance."""
        try:
            url = f"{self.base_url}/vehicles/{vin}/clearance"
            response = requests.delete(url, headers=self._get_headers())
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to delete Stellantis clearance: {str(e)}")
            return 500, str(e) 
