import logging
import requests
from datetime import datetime, timezone
from typing import Tuple, Any, List, Dict

class HMApi:
    """High Mobility API client for vehicle management."""
    
    BRAND_MAPPING = {
        'mercedes': 'mercedes-benz',
        'mercedes-benz': 'mercedes-benz'
    }
    
    STATUS_MAPPING = {
        'approved': True,
        'revoked': False,
        'pending': False
    }
    
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = None
        self.__token_exp = 0

    def _is_token_expired(self) -> bool:
        """Check if the current token is expired."""
        if not self._access_token:
            return True
        # Add a 5-minute buffer to prevent unnecessary renewals
        timestamp = (datetime.now(tz=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
        return timestamp >= (self.__token_exp - 60)  # Only renew when within 1 minute of expiration

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
            expires_in = int(response_data.get("expires_in", 3600))
            self.__token_exp = timestamp + expires_in
            logging.info("Successfully renewed High Mobility auth token")
            return self._access_token
        except Exception as e:
            logging.error(f"Failed to get HM auth token: {str(e)}")
            raise

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        # Only check token expiration if we don't have a token or if it's expired
        if not self._access_token or self._is_token_expired():
            self._get_auth_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    def _handle_auth_error(self, response: requests.Response, retry_count: int = 0) -> Tuple[int, Any]:
        """Handle authentication errors by refreshing token and retrying.
        
        Args:
            response: The response that indicated an auth error
            retry_count: Number of times this request has been retried
        
        Returns:
            Tuple of (status_code, response_data)
        """
        if response.status_code == 401 and retry_count < 1:
            logging.info("Received 401, attempting to refresh token and retry")
            self._access_token = None
            return None
        return response.status_code, response.json() if response.ok else response.text

    def get_status(self, vin: str) -> Tuple[int, Any]:
        """Get vehicle status.
        
        Returns:
            Tuple[int, Any]: A tuple containing:
                - HTTP status code
                - If successful, a dict with 'has_clearance' boolean and 'status' string
                  If failed, the error message
        """
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/fleets/vehicles/{vin}"
                response = requests.get(url, headers=self._get_headers())
                
                if response.status_code == 401:
                    result = self._handle_auth_error(response, retry_count)
                    if result is None:
                        retry_count += 1
                        continue
                
                if response.ok:
                    data = response.json()
                    status = data.get('status', '').lower()
                    if status == 'approved':
                        return True
                    else:
                        return False
                    
                else:
                    return False
            except Exception as e:
                return False

    def get_clearance(self, vin: str) -> Tuple[int, Any]:
        """Get vehicle clearance status."""
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/vehicles/{vin}/clearance"
                response = requests.get(url, headers=self._get_headers())
                
                if response.status_code == 401:
                    result = self._handle_auth_error(response, retry_count)
                    if result is None:
                        retry_count += 1
                        continue
                    return result
                
                return response.status_code, response.json() if response.ok else response.text
            except Exception as e:
                logging.error(f"Failed to get HM clearance: {str(e)}")
                return 500, str(e)

    def create_clearance(self, vehicles: List[Dict[str, str]]) -> Tuple[int, Dict[str, Any]]:
        """Create clearance for vehicles and check their real activation status.
        
        Args:
            vehicles: List of dicts containing 'vin' and 'brand' for each vehicle
            
        Returns:
            Tuple[int, Dict[str, Any]]: A tuple containing:
                - HTTP status code
                - Dict with creation response and real activation status for each VIN
        """
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/fleets/vehicles"
                formatted_vehicles = []
                for vehicle in vehicles:
                    brand = vehicle["brand"].lower()
                    formatted_vehicles.append({
                        "vin": vehicle["vin"],
                        "brand": self.BRAND_MAPPING.get(brand, brand),
                    })
                
                response = requests.post(
                    url,
                    headers=self._get_headers(),
                    json={"vehicles": formatted_vehicles}
                )
                
                if response.status_code == 401:
                    result = self._handle_auth_error(response, retry_count)
                    if result is None:
                        retry_count += 1
                        continue
                    return result

                activation_status = {}
                if response.ok:
                    for vehicle in formatted_vehicles:
                        vin = vehicle["vin"]
                        status_code, status_data = self.get_status(vin)
                        activation_status[vin] = status_data if status_code == 200 else {'has_clearance': False, 'status': 'error'}

                    return response.status_code, {
                        'creation_response': response.json(),
                        'activation_status': activation_status
                    }
                
                return response.status_code, response.text
            except Exception as e:
                logging.error(f"Failed to create HM clearance: {str(e)}")
                return 500, str(e)

    def delete_clearance(self, vin: str) -> Tuple[int, Any]:
        """Delete vehicle clearance."""
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/fleets/vehicles/{vin}"
                response = requests.delete(url, headers=self._get_headers())
                
                if response.status_code == 401:
                    result = self._handle_auth_error(response, retry_count)
                    if result is None:
                        retry_count += 1
                        continue
                    return result
                
                return response.status_code, response.json() if response.ok else response.text
            except Exception as e:
                logging.error(f"Failed to delete HM clearance: {str(e)}")
                return 500, str(e) 
