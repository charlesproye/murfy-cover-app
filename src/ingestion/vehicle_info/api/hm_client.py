import logging
import aiohttp
from datetime import datetime, timezone
import asyncio
from typing import Tuple, Any, List, Dict

class HMApi:
    """High Mobility API client for vehicle management."""
    
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

    async def _get_auth_token(self, session: aiohttp.ClientSession) -> str:
        """Get authentication token from High Mobility API."""
        try:
            response = await session.post(
                f"{self.base_url}/v1/access_tokens",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=10
            )
            response.raise_for_status()
            response_data = await response.json()
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

    async def _get_headers(self, session: aiohttp.ClientSession) -> Dict[str, str]:
        """Get headers for API requests."""
        if not self._access_token:
            self._access_token = await self._get_auth_token(session)
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    async def _handle_auth_error(self, response: aiohttp.ClientResponse, retry_count: int = 0) -> Tuple[int, Any]:
        """Handle authentication errors by refreshing token and retrying.
        
        Args:
            response: The response that indicated an auth error
            retry_count: Number of times this request has been retried
        
        Returns:
            Tuple of (status_code, response_data)
        """
        if response.status == 401 and retry_count < 1:
            logging.info("Received 401, attempting to refresh token and retry")
            self._access_token = None
            return None
        return response.status, await response.json() if response.ok else await response.text()

    async def get_status(self, vin: str, session: aiohttp.ClientSession) -> bool:
        """Get vehicle status.
        
        Returns:
            bool: True if vehicle has clearance, False otherwise
        """
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/fleets/vehicles/{vin}"
                headers = await self._get_headers(session)
                response = await session.get(url, headers=headers)
                if response.status == 401:
                    self._access_token = None
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue

                elif response.status == 200:
                    data = await response.json()
                    status = data.get('status', '').lower()
                    if status == 'approved':
                        return True
                    else:
                        return False
                
                else:
                    return False
            except Exception as e:
                return False

    async def get_clearance(self, vin: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Get vehicle clearance status."""
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/vehicles/{vin}/clearance"
                headers = await self._get_headers(session)
                response = await session.get(url, headers=headers)
                
                if response.status_code == 401:
                    result = self._handle_auth_error(response, retry_count)
                    if result is None:
                        retry_count += 1
                        continue
                    return result
                
                return response.status, response.json() if response.ok else response.text
            except Exception as e:
                logging.error(f"Failed to get HM clearance: {str(e)}")
                return 500, str(e)

    async def create_clearance(self, vin: str, brand: str, session: aiohttp.ClientSession) -> bool:
        """Create clearance for vehicles and check their real activation status.
        
        Args:
            vehicles: List of dicts containing 'vin' and 'brand' for each vehicle
            
        Returns:
            bool: True if clearance was created successfully, False otherwise
        """
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/fleets/vehicles"
                if brand == 'mercedes': brand = 'mercedes-benz'
                headers = await self._get_headers(session)
                response = await session.post(
                    url,
                    headers=headers,
                    json={"vehicles": [
                        {
                            "vin": vin,
                            "brand": brand,
                        }
                    ]}
                )
                if response.status == 401:
                    self._access_token = None
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue

                elif response.ok:
                    return await self.get_status(vin,session)
                else:
                    return False

            except Exception as e:
                return False

    async def delete_clearance(self, vin: str, session: aiohttp.ClientSession) -> bool:
        """Delete vehicle clearance."""
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/fleets/vehicles/{vin}"
                headers = await self._get_headers(session)
                response = await session.delete(url, headers=headers)
                
                if response.status == 401:
                    self._access_token = None
                    retry_count += 1
                    continue
                elif response.status in [200, 204]:
                    return True
                else:
                    return False
            except Exception as e:
                return False
            
    async def get_eligibility(self, vin: str, brand: str, session: aiohttp.ClientSession) -> bool:
        """Get vehicle eligibility."""
        retry_count = 0
        while retry_count < 2:
            try:
                url = f"{self.base_url}/v1/eligibility"
                headers = await self._get_headers(session)
                response = await session.post(url, headers=headers, json={"brand": brand, "vin": vin})
                if response.status != 200:
                    self._access_token = None
                    retry_count += 1
                    await asyncio.sleep(1)
                    continue
                if response.status == 200:
                    response_data = await response.json()
                    if response_data.get("eligible") == True:
                        return True
                    else:
                        return False
                else:
                    return False
            except Exception as e:
                return False
        
