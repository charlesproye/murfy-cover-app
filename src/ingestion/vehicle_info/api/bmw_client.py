import logging
import requests
import json
import aiohttp
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

    async def _get_auth_token(self, session: aiohttp.ClientSession) -> str:
        """Get authentication token from BMW API."""
        try:
            response = await session.post(
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
            # Handle the response text directly since it's not standard JSON MIME type
            response_text = await response.text()
            auth_result = json.loads(response_text).get("AuthenticationResult", {})
            self._access_token = auth_result.get("IdToken")
            if not self._access_token:
                raise ValueError("No IdToken found in authentication response")
            return self._access_token
        except Exception as e:
            logging.error(f"Failed to get BMW auth token: {str(e)}")
            raise

    async def _get_headers(self, session: aiohttp.ClientSession) -> Dict[str, str]:
        """Get headers for API requests."""
        if not self._access_token:
            await self._get_auth_token(session)
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    async def check_vehicle_status(self, vin: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Get vehicle clearance status."""
        try:
            url = f"{self.base_url}/vehicle/{vin}"
            headers = await self._get_headers(session)
            response = await session.get(url, headers=headers)
            if response.status == 200:
                return True
            elif response.status == 404:
                return False
            else:
                return False
        except Exception as e:
            logging.error(f"Failed to get BMW clearance: {str(e)}")
            return 500, str(e)

    async def create_clearance(self, vehicle_data: Dict[str, Any], session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Create clearance for a vehicle.
        
        Args:
            vehicle_data: Dictionary containing vehicle information (vin, licence_plate, note, contract)
        """
        try:
            url = f"{self.base_url}/vehicle"
            vehicle_data['contract']['end_date'] = ""
            headers = await self._get_headers(session)
            payload = json.dumps(vehicle_data)
                        
            response = await session.post(
                url,
                headers=headers,
                data=payload
            )
            return response.status, await response.json() if response.ok else await response.text()
        except Exception as e:
            logging.error(f"Failed to create BMW clearance: {str(e)}")
            return 500, str(e)

    async def deactivate(self, vin: str, session: aiohttp.ClientSession) -> bool:
        """Delete vehicle clearance."""
        try:
            url = f"{self.base_url}/vehicle/{vin}"
            headers = await self._get_headers(session)
            response = await session.delete(url, headers=headers)
            if response.status in [200, 204]:
                return True
            else:
                return False
        except Exception as e:
            return False

    async def get_fleets(self, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Get list of available fleets."""
        try:
            url = f"{self.base_url}/fleet"
            headers = await self._get_headers(session)
            response = await session.get(url, headers=headers)
            return response.status, await response.json() if response.ok else await response.text()
        except Exception as e:
            logging.error(f"Failed to get BMW fleets: {str(e)}")
            return 500, str(e)

    async def add_vehicle_to_fleet(self, fleet_id: str, vin: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Add a vehicle to a fleet.
        
        Args:
            fleet_id: The ID of the fleet to add the vehicle to
            vin: The VIN of the vehicle to add
        """
        try:
            url = f"{self.base_url}/fleet/{fleet_id}/vehicle/{vin}"
            headers = await self._get_headers(session)
            response = await session.post(url, headers=headers)
            return response.status, await response.text() if response.text else ""
        except Exception as e:
            logging.error(f"Failed to add vehicle to BMW fleet: {str(e)}")
            return 500, str(e) 
