import logging
from datetime import datetime

import aiohttp
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel

from core.tesla.tesla_utils import FLEET_URLS, TeslaRegions


class TeslaTokens(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int


class TeslaIndividualApi:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        region: TeslaRegions,
        session: aiohttp.ClientSession,
    ):
        self.base_url = FLEET_URLS[region]
        self.token_url = "https://fleet-auth.prd.vn.cloud.tesla.com"
        self.client_id = client_id
        self.client_secret = client_secret
        self.logger = logging.getLogger(__name__)
        self.session = session

    async def get_token(
        self,
        code: str,
        redirect_uri: str,
        region: TeslaRegions,
    ) -> TeslaTokens:
        payload = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
            "audience": FLEET_URLS[region],
            "redirect_uri": redirect_uri,
        }

        async with self.session.post(
            f"{self.token_url}/oauth2/v3/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
        ) as response:
            if response.status != 200:
                text = await response.text()
                self.logger.error(f"Error getting tokens from code: {text}")
                raise Exception(f"Error getting tokens from code: {text}")

            data = await response.json()
            return TeslaTokens(**data)

    async def refresh_token(self, refresh_token: str) -> TeslaTokens | None:
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "refresh_token": refresh_token,
        }

        async with self.session.post(
            f"{self.token_url}/oauth2/v3/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
        ) as response:
            if response.status != 200:
                text = await response.text()
                self.logger.error(f"Error refreshing tokens: {text}")
                return None

            data = await response.json()
            return TeslaTokens(**data)

    async def get_version(self, vin: str, access_token: str) -> str:
        url = f"{self.base_url}/api/1/dx/vehicles/options?vin={vin}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        async with self.session.get(url, headers=headers) as response:
            if response.status != 200:
                self.logger.warning(f"Failed to get version for VIN {vin}")
                return "MTU"

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
                return "MTU"

            version = model_info["code"][1:]
            if version == "MTY13":
                version = "MTY13C" if vin[10] == "C" else "MTY13B"

            return version

    async def get_start_date(self, vin: str, access_token: str) -> str | None:
        url = f"{self.base_url}/api/1/dx/warranty/details?vin={vin}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        async with self.session.get(url, headers=headers) as response:
            if response.status != 200:
                self.logger.warning(f"Failed to get start date for VIN {vin}")
                return None

            data = await response.json()
            active_warranty = data.get("activeWarranty", [])
            if active_warranty:
                warranty = active_warranty[1]
                expiration_date = warranty.get("expirationDate")
                warranty_age_years = warranty.get("coverageAgeInYears")

                expiration_date_obj = datetime.fromisoformat(
                    expiration_date.replace("Z", "+00:00")
                )
                start_date_obj = expiration_date_obj - relativedelta(
                    years=int(warranty_age_years)
                )
                return start_date_obj.strftime("%Y-%m-%d")
            return None
