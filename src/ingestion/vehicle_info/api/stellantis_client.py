import logging
import aiohttp
import uuid
from datetime import datetime, timezone
from typing import Tuple, Any, List, Dict
import json

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
        """Check if a vehicle is eligible for activation."""
        try:
            url = f"{self.base_url}/connected-fleet/api/vehicles/eligibilities"
            response = await session.post(
                url, 
                headers=await self._get_headers(session), 
                json={"vins": [vin], "resultByEmail": False}
            )
            if response.status == 200:
                response_data = await response.json()
                if response_data and isinstance(response_data, list) and len(response_data) > 0:
                    vehicle_data = response_data[0]
                    is_eligible = vehicle_data.get("eligible", False)
                    return is_eligible
                else:
                    logging.error("Unexpected response structure: not a list or empty list")
                    return False
            else:
                logging.error(f"Failed to check Stellantis eligibility vin: {vin}")
                return False
        except Exception as e:
            logging.error(f"Failed to check Stellantis eligibility vin: {vin}")
            return False

    async def get_status(self, vin: str, session: aiohttp.ClientSession, skip: int = 0, limit: int = 100) -> Tuple[int, Any]:
        """Get vehicle contracts."""
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts"
            
            conditions = json.dumps({"car.vin": vin})
            
            params = {
                "skip": skip,
                "limit": limit,
                "conditions": conditions
            }
            
            response = await session.get(
                url, 
                headers=await self._get_headers(session),
                params=params
            )

            if response.ok:
                data = await response.json()
                if data and isinstance(data, list) and len(data) > 0:
                    return response.status, data[0]
                return 404, None
            return response.status, await response.text()
        except Exception as e:
            logging.error(f"Failed to get Stellantis vehicle contracts: {str(e)}")
            return 500, str(e)

    async def create_clearance(self, vehicles: List[Dict[str, str]], session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Create clearance for vehicles."""
        try:
            if not vehicles:
                return 400, "No vehicles provided"
                
            vehicle = vehicles[0]
            vin = vehicle["vin"]
            
            if not await self.is_eligible(vin, session):
                error_msg = f"Vehicle {vin} is not eligible for activation"
                logging.error(error_msg)
                return 400, error_msg
            
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
            
            response = await session.post(
                url,
                headers=await self._get_headers(session),
                json=data
            )
            return response.status, await response.json() if response.ok else await response.text()
        except Exception as e:
            logging.error(f"Failed to create Stellantis clearance: {str(e)}")
            return 500, str(e)

    async def delete_clearance(self, contract_id: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
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
            error_msg = str(e)
            logging.error(f"Failed to delete Stellantis clearance: {error_msg}")
            return 500, {"message": error_msg}

    async def add_to_fleet(self, fleet_id: str, vin: str, session: aiohttp.ClientSession) -> Tuple[int, Any]:
        """Add a vehicle to a fleet."""
        try:
            url = f"{self.base_url}/connected-fleet/api/fleets/{fleet_id}/vehicles"
            
            data = {
                "vins": [vin]
            }
            
            response = await session.put(
                url,
                headers=await self._get_headers(session),
                json=data
            )
            return response.status, await response.json() if response.ok else await response.text()
        except Exception as e:
            logging.error(f"Failed to add vehicle to fleet: {str(e)}")
            return 500, str(e) 
