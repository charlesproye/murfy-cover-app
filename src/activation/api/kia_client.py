import logging
import aiohttp
from typing import List, Dict
from activation.utils.eligibility_checker.utils import require_valid_vins


class KiaApi:
    """Kia API client for vehicle management."""
    
    def __init__(self, auth_url: str, base_url: str, client_username: str,
                 client_pwd: str, api_key: str):
        self.auth_url = auth_url
        self.base_url = base_url
        self.client_username = client_username
        self.client_pwd = client_pwd
        self.api_key = api_key
        self._access_token = None
        self.logger = logging.getLogger(' KIA Api')
        self.logger.setLevel(logging.INFO)
        self.scope = "external-server-sta/default"

        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    async def _get_auth_token(self, session: aiohttp.ClientSession) -> str:
        """Get authentication token from Cognito."""
        try:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_username,
                "client_secret": self.client_pwd,
                "scope": self.scope
            }

            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
            }

            async with session.post(self.auth_url, headers=headers, data=payload) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("access_token")
        except Exception as e:
            print("Error fetching token:", e)
            return None

    @require_valid_vins
    async def _activate_vins(self, session: aiohttp.ClientSession, vins: list):
        """Send VIN consent request to the API."""

        if not self._access_token:
            self._access_token = await self._get_auth_token(session)

        payload = {
            "vinList": [{"vin": vin} for vin in vins],
            "consentType": ["vehicle", "dtcInfo"],
            "apiType": "real_time"
        }

        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self._access_token}"
        }

        try:
            async with session.post(f"{self.base_url}/consent", headers=headers, json=payload) as response:
                response.raise_for_status()

                return await response.json()
        except Exception as e:
            print("Error sending consent request:", e)
            return None

    async def _get_status_consent_type(self, session: aiohttp.ClientSession, consent_type: str):
        """Get status of consent type for a list of vins."""

        if not self._access_token:
            self._access_token = await self._get_auth_token(session)
        
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self._access_token}"
        }

        try:
            async with session.get(f"{self.base_url}/consent/{consent_type}", headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            print(f"Error getting status for package {consent_type}:", e)
            return None


    async def delete_consent(self, session: aiohttp.ClientSession, vins: str):
        """Revoke a consent for a given VIN and consent type."""

        if not self._access_token:
            self._access_token = await self._get_auth_token(session)

        url = f"{self.base_url}/consent"

        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self._access_token}"
        }

        payload = {
            "vinList": [{"vin": vin} for vin in vins],
            "consentType": ["vehicle", "dtcInfo"]
        }

        try:
            async with session.delete(url, headers=headers, json=payload) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            print("Error deleting consent:", e)
            return None
