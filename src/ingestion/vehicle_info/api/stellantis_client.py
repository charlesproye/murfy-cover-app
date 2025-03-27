import logging
import aiohttp
import uuid
from datetime import datetime, timezone
from typing import Tuple, Any, List, Dict, Optional
import json
import asyncio
class StellantisApi:
    """Stellantis API client for vehicle management."""
    
    def __init__(self, base_url: str, email: str, password: str, fleet_id: str, company_id: str):
        self.base_url = base_url.rstrip('/')
        self.email = email
        self.password = password
        self.fleet_id = fleet_id
        self.company_id = company_id
        self._access_token = None

    async def _get_auth_token(self, session: aiohttp.ClientSession) -> str:
        """Get authentication token from Stellantis API."""
        try:
            response = await session.post(
                f"{self.base_url}/api/auth/login",
                json={
                    "email": self.email,
                    "password": self.password
                },
                headers={
                    'Content-Type': 'application/json'
                },
                timeout=10
            )
            response.raise_for_status()
            response_data = await response.json()
            self._access_token = response_data.get("authToken")
            return self._access_token
        except Exception as e:
            logging.error(f"Failed to get Stellantis auth token: {str(e)}")
            raise

    async def _get_headers(self, session: aiohttp.ClientSession) -> Dict[str, str]:
        """Get headers for API requests."""
        if not self._access_token:
            await self._get_auth_token(session)
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }
    
    async def is_eligible(self, vin: str, session: aiohttp.ClientSession) -> bool:
        """Check if a vehicle is eligible for activation.
        
        Args:
            vin: Vehicle Identification Number
            session: aiohttp ClientSession for making HTTP requests
            
        Returns:
            bool: True if the vehicle is eligible, False otherwise
        """
        try:
            url = f"{self.base_url}/connected-fleet/api/vehicles/eligibilities"
            response = await session.post(
                url, 
                headers=await self._get_headers(session), 
                json={"vins": [vin], "resultByEmail": False}
            )
            if response.status == 502:
                await asyncio.sleep(2)
                response = await session.post(url, headers=await self._get_headers(session), json={"vins": [vin], "resultByEmail": False})
            response.raise_for_status()
            
            response_data = await response.json()
            if not response_data or not isinstance(response_data, list) or not response_data:
                return False
                
            vehicle_data = response_data[0]
            return vehicle_data.get("eligible") is True

        except Exception as e:
            logging.error(f"Failed to check eligibility for VIN {vin}: {str(e)}")
            return False

    async def get_status(self, vin: str, session: aiohttp.ClientSession, skip: int = 0, limit: int = 100) -> Tuple[bool, Optional[str]]:
        """Get vehicle activation status and contract ID."""
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts"
            params = {
                "skip": skip,
                "limit": limit,
                "conditions": json.dumps({"car.vin": vin})
            }
            
            response = await session.get(url, headers=await self._get_headers(session), params=params)
            if response.status == 502:
                await asyncio.sleep(2)
                response = await session.get(url, headers=await self._get_headers(session), params=params)
            response.raise_for_status()
            
            data = await response.json()
            if not data or not isinstance(data, list):
                return False, None
                
            contract = data[0]
            return contract.get("status") == "activated", contract.get("_id",None)
            
        except Exception as e:
            logging.error(f"Failed to get vehicle status for VIN {vin}: {str(e)}")
            return False, None

    async def activate(self, vin: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Create clearance for vehicles."""
        try:

            url = f"{self.base_url}/connected-fleet/api/contracts"
            
            data = {
                "reference": str(uuid.uuid4()),
                "company": self.company_id,
                "car": {
                    "vin": vin,
                    "imei": str(uuid.uuid4()),
                    "note": ""
                },
                "from": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": datetime.now(timezone.utc).replace(year=datetime.now().year + 1).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "pack": "pack-1"
            }
            response = await session.post(url,headers=await self._get_headers(session),json=data)
            return response.status, await response.json() if response.ok else await response.text()
        except Exception as e:
            logging.error(f"Failed to create Stellantis clearance: {str(e)}")
            return 500, str(e)

    async def deactivate(self, contract_id: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Delete vehicle clearance."""
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts/{contract_id}"
            response = await session.delete(url, headers=await self._get_headers(session))
            
            if response.status == 204:
                return response.status, None
                
            try:
                error_data = await response.json() if await response.text() else {"message": "No error details available"}
            except json.JSONDecodeError:
                error_data = {"message": await response.text() or "No error details available"}
            
            if response.status == 400:
                logging.error(f"Bad request deleting contract {contract_id}: {error_data}")
            elif response.status == 404:
                logging.error(f"Contract {contract_id} not found: {error_data}")
            elif response.status == 500:
                logging.error(f"Server error deleting contract {contract_id}: {error_data}")
            else:
                logging.error(f"Unexpected status code {response.status} deleting contract {contract_id}: {error_data}")
            
            return response.status, error_data
            
        except Exception as e:
            logging.error(f"Failed to delete Stellantis clearance: {str(e)}")
            return 500, str(e)
