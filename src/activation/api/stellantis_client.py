import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import aiohttp


class StellantisApi:
    """Stellantis API client for vehicle management."""

    def __init__(self, base_url: str, email: str, password: str, company_id: str):
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self.fleet_id = "6863afa8a35b840007fb7ae3"  # Bib fleet
        self.company_id = company_id
        self._access_token = None

    async def _get_auth_token(self, session: aiohttp.ClientSession) -> str:
        """Get authentication token from Stellantis API."""
        try:
            response = await session.post(
                f"{self.base_url}/api/auth/login",
                json={"email": self.email, "password": self.password},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            response_data = await response.json()
            self._access_token = response_data.get("authToken")
            return self._access_token
        except Exception as e:
            logging.error(f"Failed to get Stellantis auth token: {e}")
            raise

    async def _get_headers(self, session: aiohttp.ClientSession) -> dict[str, str]:
        """Get headers for API requests."""
        if not self._access_token:
            await self._get_auth_token(session)
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
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
                json={"vins": [vin], "resultByEmail": False},
            )
            if response.status == 502:
                await asyncio.sleep(2)
                response = await session.post(
                    url,
                    headers=await self._get_headers(session),
                    json={"vins": [vin], "resultByEmail": False},
                )
            response.raise_for_status()

            response_data = await response.json()
            if (
                not response_data
                or not isinstance(response_data, list)
                or not response_data
            ):
                return False

            vehicle_data = response_data[0]
            return vehicle_data.get("eligible") is True

        except Exception as e:
            logging.error(f"Failed to check eligibility for VIN {vin}: {e}")
            return False

    async def get_status(
        self, vin: str, session: aiohttp.ClientSession, skip: int = 0, limit: int = 100
    ) -> tuple[bool, str, str]:
        """Get vehicle activation status and contract ID."""
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts"
            params = {
                "skip": skip,
                "limit": limit,
                "conditions": json.dumps({"car.vin": vin}),
            }

            response = await session.get(
                url, headers=await self._get_headers(session), params=params
            )
            if response.status == 502:
                await asyncio.sleep(2)
                response = await session.get(
                    url, headers=await self._get_headers(session), params=params
                )
            response.raise_for_status()

            data = await response.json()
            if not data or not isinstance(data, list):
                return False, None, None

            for i in range(len(data)):
                if data[i].get("status") == "activated":
                    return True, data[i].get("_id", None), data[i].get("status", None)
                elif (
                    data[i].get("status") == "pending"
                    or data[i].get("status") == "rejected"
                ):
                    return False, None, data[i].get("status", None)

            return False, None, None

        except Exception as e:
            logging.error(f"Failed to get vehicle status for VIN {vin}: {e}")
            return False, None, None

    async def add_to_fleet(self, id: str, session: aiohttp.ClientSession):
        """Add a vehicle to the fleet."""

        url = f"{self.base_url}/connected-fleet/api/fleets/{self.fleet_id}/vehicles"
        data = [f"{id}"]

        response = await session.put(
            url, headers=await self._get_headers(session), json=data
        )

        return response.status

    async def activate(
        self, vin: str, session: aiohttp.ClientSession
    ) -> tuple[int, Any]:
        """Create clearance for vehicles."""
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts"

            data = {
                "reference": str(uuid.uuid4()),
                "company": self.company_id,
                "car": {"vin": vin, "imei": str(uuid.uuid4()), "note": ""},
                "from": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": datetime.now(UTC)
                .replace(year=datetime.now().year + 1)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "pack": "pack-1",
            }
            response = await session.post(
                url, headers=await self._get_headers(session), json=data
            )

            if response.status == 201:
                json_data = await response.json()  # await la coroutine d'abord
                id = json_data["_id"]
                status_fleet = await self.add_to_fleet(id, session)
                if status_fleet == 204:
                    logging.info(f"Vehicle {vin} added to fleet successfully")

            return (
                response.status,
                await response.json() if response.ok else await response.text(),
            )
        except Exception as e:
            logging.error(f"Failed to create Stellantis clearance: {e}")
            return 500, e

    async def deactivate(
        self, contract_id: str, session: aiohttp.ClientSession
    ) -> tuple[int, Any]:
        """Delete vehicle clearance."""
        try:
            url = f"{self.base_url}/connected-fleet/api/contracts/{contract_id}"
            response = await session.delete(
                url, headers=await self._get_headers(session)
            )

            if response.status == 204:
                return response.status, None

            try:
                error_data = (
                    await response.json()
                    if await response.text()
                    else {"message": "No error details available"}
                )
            except json.JSONDecodeError:
                error_data = {
                    "message": await response.text() or "No error details available"
                }

            if response.status == 400:
                logging.error(
                    f"Bad request deleting contract {contract_id}: {error_data}"
                )
            elif response.status == 404:
                logging.error(f"Contract {contract_id} not found: {error_data}")
            elif response.status == 500:
                logging.error(
                    f"Server error deleting contract {contract_id}: {error_data}"
                )
            else:
                logging.error(
                    f"Unexpected status code {response.status} deleting contract {contract_id}: {error_data}"
                )

            return response.status, error_data

        except Exception as e:
            logging.error(f"Failed to delete Stellantis clearance: {e}")
            return 500, e

