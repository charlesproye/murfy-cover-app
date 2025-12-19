import logging
import re
import uuid
from datetime import datetime
from typing import ClassVar

import aiohttp
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from core.encrypt_utils import Encrypter
from db_models.vehicle import FleetTeslaAuthenticationCode


class TeslaApi:
    """Tesla Fleet API client for vehicle management."""

    # Patterns to retrieve the vehicle type and version from the Tesla response architecture
    TESLA_PATTERNS: ClassVar[dict[str, dict[str, list[tuple[str, str]]]]] = {
        "model 3": {
            "patterns": [
                (
                    r".*standard range.*plus.*rear.?wheel.*|.*standard range.*plus.*rwd.*|.*rear.?wheel drive.*",
                    "rwd",
                ),
                (r".*performance.*dual motor.*|.*performance.*", "performance"),
                (r".*long range.*all.?wheel drive.*", "long range awd"),
            ]
        },
        "model s": {
            "patterns": [
                (r".*100d.*", "100d"),
                (r".*75d.*", "75d"),
                (r".*long range.*plus.*", "long range plus"),
                (r".*long range.*", "long range"),
                (r".*plaid.*", "plaid"),
                (r".*performance.*", "performance"),
                (r".*standard range.*", "standard range"),
            ]
        },
        "model x": {
            "patterns": [
                (r".*long range.*plus.*", "long range plus"),
                (r".*long range.*", "long range"),
            ]
        },
        "model y": {
            "patterns": [
                (r".*long range.*rwd.*", "long range rwd"),
                (r".*long range.*all.?wheel drive.*", "long range awd"),
                (r".*performance.*awd.*", "performance"),
                (r".*rear.?wheel drive.*", "rwd"),
            ]
        },
    }

    AUTH_URL = "https://fleet-auth.prd.vn.cloud.tesla.com/oauth2/v3/token"
    BASE_URL = "https://fleet-api.prd.eu.vn.cloud.tesla.com"
    OAUTH_SCOPE = "openid offline_access user_data vehicle_device_data vehicle_cmds vehicle_charging_cmds energy_device_data"

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tokens: dict[str, str] = {}
        self.encrypter = Encrypter()

    def _get_headers(self, token: str) -> dict[str, str]:
        """Build HTTP headers with authorization token."""
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _fetch_access_token(
        self, session: aiohttp.ClientSession, authentication_code: str
    ) -> dict:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": self.BASE_URL,
            "auth_code": authentication_code,
            "scope": self.OAUTH_SCOPE,
        }

        async with session.post(self.AUTH_URL, headers=headers, data=data) as response:
            if response.status == 200:
                return await response.json()

            error_text = await response.text()
            raise ValueError(f"Failed to fetch token: {response.status} - {error_text}")

    async def fetch_all_tokens(
        self, session: aiohttp.ClientSession, db: AsyncSession
    ) -> None:
        authentication_codes = await db.execute(
            select(
                FleetTeslaAuthenticationCode.fleet_id,
                FleetTeslaAuthenticationCode.authentication_code,
            )
        )

        authentication_codes = authentication_codes.all()
        authentication_codes = [
            (fleet_id, self.encrypter.decrypt(auth_code))
            for fleet_id, auth_code in authentication_codes
        ]
        for fleet_id, auth_code in authentication_codes:
            token_data = await self._fetch_access_token(session, auth_code)
            self.tokens[fleet_id] = token_data["access_token"]
            logging.info(f"Fetched token for fleet {fleet_id}")

    async def get_warranty_info(
        self, session: aiohttp.ClientSession, vin: str, fleet_id: uuid.UUID
    ) -> tuple[int | None, int | None, str | None]:
        access_token = self.tokens.get(fleet_id)

        if not access_token:
            logging.warning(f"No token found for fleet {fleet_id}")
            return None, None, None

        url = f"{self.BASE_URL}/api/1/dx/warranty/details?vin={vin}"
        headers = self._get_headers(access_token)

        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logging.warning(
                    f"HTTP {response.status} error fetching warranty for VIN {vin}"
                )
                return None, None, None

            data = await response.json()
            active_warranty = data.get("activeWarranty", [])

            if not active_warranty or len(active_warranty) < 2:
                logging.warning(f"No valid warranty data for VIN {vin}")
                return None, None, None

            warranty = active_warranty[1]
            expiration_date = warranty.get("expirationDate")
            warranty_years = warranty.get("coverageAgeInYears")
            warranty_km = int(warranty.get("expirationOdometer", 0))

            # Tesla uses 9999999 for unlimited warranty
            warranty_km = 240000 if warranty_km == 9999999 else warranty_km

            expiration_date_obj = datetime.fromisoformat(
                expiration_date.replace("Z", "+00:00")
            )
            start_date_obj = expiration_date_obj - relativedelta(
                years=int(warranty_years)
            )
            start_date = start_date_obj.strftime("%Y-%m-%d")

            return warranty_km, warranty_years, start_date

    async def get_vehicle_options(
        self,
        session: aiohttp.ClientSession,
        vin: str,
        fleet_id: uuid.UUID,
        model_name: str,
    ) -> tuple[str, str | None]:
        access_token = self.tokens.get(fleet_id)

        if not access_token:
            logging.warning(f"No token found for fleet {fleet_id}")
            return "MTU", None

        url = f"{self.BASE_URL}/api/1/dx/vehicles/options?vin={vin}"
        headers = self._get_headers(access_token)

        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logging.warning(
                    f"Failed to fetch options for VIN {vin}: HTTP {response.status}"
                )
                return "MTU", None

            data = await response.json()
            model_info = next(
                (
                    item
                    for item in data.get("codes", [])
                    if item["code"].startswith("$MT")
                ),
                None,
            )

            if not model_info:
                return "MTU", None

            # Extract version code
            version = model_info["code"][1:]  # Remove $ prefix
            if version == "MTY13":
                # Distinguish between MTY13B and MTY13C based on VIN
                version = "MTY13C" if vin[10] == "C" else "MTY13B"

            # Determine vehicle type using pattern matching
            display_name = model_info["displayName"].lower()
            if model_name not in self.TESLA_PATTERNS:
                return version, None

            vehicle_type = next(
                (
                    type_name
                    for pattern, type_name in self.TESLA_PATTERNS[model_name][
                        "patterns"
                    ]
                    if re.match(pattern, display_name, re.IGNORECASE)
                ),
                None,
            )

            return version, vehicle_type
