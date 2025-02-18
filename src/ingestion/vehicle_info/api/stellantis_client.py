import logging
import requests
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

    def _get_auth_token(self) -> str:
        """Get authentication token from Stellantis API."""
        try:
            response = requests.post(
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
            response_data = response.json()
            self._access_token = response_data.get("authToken")
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
    
    def is_eligible(self, vin: str) -> bool:
        """Check if a vehicle is eligible for activation."""
        try:
            url = f"{self.base_url}/connected-fleet/api/vehicles/eligibilities"
            response = requests.post(
                url, 
                headers=self._get_headers(), 
                json={"vins": [vin], "resultByEmail": False}
            )
            if response.status_code == 200:
                response_data = response.json()
                if response_data and isinstance(response_data, list) and len(response_data) > 0:
                    vehicle_data = response_data[0]
                    is_eligible = vehicle_data.get("eligible", False)
                    if not is_eligible:
                        logging.info(f"Vehicle {vin} not eligible")
                    return is_eligible
                else:
                    logging.error("Unexpected response structure: not a list or empty list")
                    return False
            else:
                logging.error(f"Failed to check Stellantis eligibility: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Failed to check Stellantis eligibility: {str(e)}")
            return False

    def get_status(self, vin: str, skip: int = 0, limit: int = 100) -> Tuple[int, Any]:
        """Get vehicle contracts.
        
        Args:
            vin: Vehicle identification number
            skip: Number of items to skip (pagination)
            limit: Maximum number of items to return (pagination)
            
        Returns:
            Tuple of (status_code, response_data)
            response_data will contain contract info including _id if found
        """
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts"
            
            conditions = json.dumps({"car.vin": vin})
            
            params = {
                "skip": skip,
                "limit": limit,
                "conditions": conditions
            }
            
            response = requests.get(
                url, 
                headers=self._get_headers(),
                params=params
            )

            if response.ok:
                data = response.json()
                if data and isinstance(data, list) and len(data) > 0:
                    return response.status_code, data[0]
                return 404, None
            return response.status_code, response.text
        except Exception as e:
            logging.error(f"Failed to get Stellantis vehicle contracts: {str(e)}")
            return 500, str(e)

    def create_clearance(self, vehicles: List[Dict[str, str]]) -> Tuple[int, Any]:
        """Create clearance for vehicles.
        
        Args:
            vehicles: List of dicts containing 'vin' for each vehicle
            
        Returns:
            Tuple of (status_code, response_data)
            If vehicle is not eligible, returns (400, error_message)
        """
        try:
            if not vehicles:
                return 400, "No vehicles provided"
                
            vehicle = vehicles[0]
            vin = vehicle["vin"]
            
            if not self.is_eligible(vin):
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
            
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=data
            )
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to create Stellantis clearance: {str(e)}")
            return 500, str(e)

    def delete_clearance(self, contract_id: str) -> Tuple[int, Any]:
        """Delete vehicle clearance.
        
        Args:
            contract_id: The ID of the contract to delete (_id field from get_status)
            
        Returns:
            Tuple[int, Any]: Status code and response data. For successful deletion (204),
            response data will be None. For errors (400, 404, 500), response data will
            contain error details if available.
        """
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts/{contract_id}"
            response = requests.delete(url, headers=self._get_headers())
            
            if response.status_code == 204:
                return response.status_code, None
                
            try:
                error_data = response.json() if response.text else {"message": "No error details available"}
            except json.JSONDecodeError:
                error_data = {"message": response.text or "No error details available"}
            
            if response.status_code == 400:
                logging.error(f"Bad request deleting contract {contract_id}: {error_data}")
            elif response.status_code == 404:
                logging.error(f"Contract {contract_id} not found: {error_data}")
            elif response.status_code == 500:
                logging.error(f"Server error deleting contract {contract_id}: {error_data}")
            else:
                logging.error(f"Unexpected status code {response.status_code} deleting contract {contract_id}: {error_data}")
            
            return response.status_code, error_data
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Failed to delete Stellantis clearance: {error_msg}")
            return 500, {"message": error_msg}

    def add_to_fleet(self, fleet_id: str, vin: str) -> Tuple[int, Any]:
        """Add a vehicle to a fleet.
        
        Args:
            fleet_id: The ID of the fleet
            vin: Vehicle identification number
            
        Returns:
            Tuple of (status_code, response_data)
        """
        try:
            url = f"{self.base_url}/connected-fleet/api/fleets/{fleet_id}/vehicles"
            
            data = {
                "vins": [vin]
            }
            
            response = requests.put(
                url,
                headers=self._get_headers(),
                json=data
            )
            return response.status_code, response.json() if response.ok else response.text
        except Exception as e:
            logging.error(f"Failed to add vehicle to fleet: {str(e)}")
            return 500, str(e) 
