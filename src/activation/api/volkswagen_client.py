import logging
import aiohttp
from typing import List, Dict
from activation.utils.eligibility_checker.utils import require_valid_vins


class VolkswagenApi:
    """Volkswagen API client for vehicle management."""
    
    def __init__(self, auth_url: str, base_url: str, organization_id: str,
                 client_username: str, client_password: str):
        self.auth_url = auth_url
        self.base_url = base_url
        self.client_username = client_username
        self.organization_id = organization_id
        self.client_password = client_password
        self._access_token = None
        self.logger = logging.getLogger(' VolkswagenAPI')
        self.logger.setLevel(logging.INFO)

        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    async def _get_auth_token(self, session: aiohttp.ClientSession) -> str:
        """Get authentication token from Volkswagen Fleet API."""
        try:
            response = await session.post(
                f"{self.auth_url}/v1/fdp/login/third-party-system",
                headers={
                    "Content-Type": "application/json"
                },
                json={
                    "username": self.client_username,
                    "password": self.client_password
                }
            )

            response.raise_for_status()

            data = await response.json()

            self._access_token = data.get("value")

            if not self._access_token:
                raise ValueError(f"No access_token found in Volkswagen response: {data}")

            return self._access_token

        except Exception as e:
            self.logger.error(f"Failed to get Volkswagen auth token: {str(e)}")
            raise

    @require_valid_vins
    async def _get_vehicle_approval(self, session: aiohttp.ClientSession, vins: List[str]) -> Dict[str, str]:
        """
        Request vehicle approval (activation) for a list of VINs.

        Returns a dict with VIN as key and verification-code as value for approved VINs.
        Logs VINs that were not approved.
        """
        
        if not self._access_token:
            await self._get_auth_token(session)

        url = f"{self.base_url}/v1/fdp/vehicle-approval"
        headers = {
            "x-sub-organisation-id": self.organization_id,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token}"
        }
        payload = {"vins": vins}

        try:
            response = await session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = await response.json()

            approved = [
                {"vin": v["vin"], "verification-code": v["verification-code"]}
                for v in data.get("vehicle-verification-codes", [])
                if v["vin"] in vins
            ]

            already_confirmed = await self.get_confirmed_vehicles(session)

            missing = set(vins) - set([approved_v["vin"] for approved_v in approved])

            missing = set(missing) - set(already_confirmed)

            already_confirmed = set(already_confirmed).intersection(set(vins))


            if already_confirmed:
                self.logger.info(f"The following VINs were already confirmed: {already_confirmed}")

            if missing:
                self.logger.info(f"The following VINs were not approved: {missing}")

            return approved

        except aiohttp.ClientResponseError as e:
            self.logger.error(f"Vehicle approval request failed ({e.status}): {e.message}")
            return {vin: "error" for vin in vins}
        except Exception as e:
            self.logger.error(f"Unexpected error during vehicle approval: {str(e)}")
            return {vin: "error" for vin in vins}
    
    async def _get_vehicle_confirmation(
        self, 
        session: aiohttp.ClientSession, 
        response_approval: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Confirm vehicle registrations using the verification codes obtained from _get_vehicle_approval.

        Returns a dict with VIN as key and 'confirmed' or 'error' as value.
        Logs VINs that were not confirmed.
        """

        if not self._access_token:
            await self._get_auth_token(session)

        url = f"{self.base_url}/v1/fdp/vehicle-confirmation"
        headers = {
            "x-sub-organisation-id": self.organization_id,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token}"
        }

        # Préparer la liste des confirmations
        payload = {
            "vehicle-confirmations": [
                {"vin": el['vin'], "verification-code": el['verification-code']} 
                for el in response_approval
            ]
        }

        try:
            response = await session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            for el in response_approval:
                self.logger.info(f"Vehicle confirmation request successful for vin : {el['vin']}")


        except Exception as e:
            self.logger.error(f"Vehicle confirmation request failed: {str(e)}")
            return {vin: "error" for vin in response_approval.keys()}


    async def get_confirmed_vehicles(self, session: aiohttp.ClientSession) -> dict[str, str]:
        """
        Fetch existing vehicle approvals (VINs and verification codes) from VW Fleet API.

        Returns a dict with VIN as key and verification-code as value.
        """

        if not self._access_token:
            await self._get_auth_token(session)

        url = f"{self.base_url}/v1/fdp/vehicle-approval"
        headers = {
            "x-sub-organisation-id": self.organization_id,
            "Authorization": f"Bearer {self._access_token}"
        }

        try:
            response = await session.get(url, headers=headers)
            response.raise_for_status()
            data = await response.json()

            # Parse into dict {vin: verification-code}
            confirmed_vehicles = [
                item["vin"]
                for item in data.get("vehicle-confirmations", [])
                if  item['confirmation-status'] == 'confirmed'
            ]

            return set(confirmed_vehicles)

        except aiohttp.ClientResponseError as e:
            self.logger.error(f"Get vehicle approvals failed ({e.status}): {e.message}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error while getting vehicle approvals: {str(e)}")
            return {}

    @require_valid_vins
    async def check_vehicle_status(
        self, 
        session: aiohttp.ClientSession, 
        vins: list[str]
    ) -> dict[str, str]:
        """
        Check the registration/activation status of a list of vehicles.

        Returns a dict with VIN as key and status as value.
        Logs an error if the request fails.
        """
        if not self._access_token:
            await self._get_auth_token(session)

        url = f"{self.base_url}/v1/fdp/vehicle-status"
        headers = {
            "x-sub-organisation-id": self.organization_id,
            "Authorization": f"Bearer {self._access_token}"
        }

        try:
            response = await session.get(url, headers=headers, json={})
            response.raise_for_status()
            data = await response.json()

            results = {}

            results = {item["vin"]: {'status_bool':item["status"] == 'enrollment_completed', 'status':item["status"], 'reason':item["enrollment-rejection-reason"]} for item in data if item["vin"] in vins}
            for vin in vins:
                if vin not in results:
                    results[vin] = {'status_bool':False, 'status':None, 'reason':None}

            return results

        except Exception as e:
            self.logger.error(f"Vehicle status check failed: {str(e)}")
            return {vin: "error" for vin in vins}

    async def activate_vehicles(
        self,
        session: aiohttp.ClientSession,
        vins: list[str]
    ) -> dict[str, str]:
        """
        Activate vehicles by first requesting approval and then confirming them.

        Returns a dict with VIN as key and 'confirmed' or 'error' as value.
        """
        try:
            # Step 1: Approval
            approval_result = await self._get_vehicle_approval(session, vins)

            # Step 2: Confirmation
            confirmation_result = await self._get_vehicle_confirmation(session, approval_result)

            return confirmation_result

        except Exception as e:
            self.logger.error(f"Vehicle activation failed: {str(e)}")
            return {vin: "error" for vin in vins}


    @require_valid_vins
    async def deactivate_vehicles(
        self, 
        session: aiohttp.ClientSession, 
        vins: list[str]
    ) -> dict:
        """
        Deactivate (de-enroll) vehicles from VW Fleet API.

        Logs and returns the categories of VINs:
        - deenrollment-candidates
        - scheduled-deenrollment-candidates
        - ignored-vins
        """
        if not self._access_token:
            await self._get_auth_token(session)

        url = f"{self.base_url}/v1/fdp/vehicle-approval"
        headers = {
            "x-sub-organisation-id": self.organization_id,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token}"
        }
        payload = {"vins": vins}

        try:
            response = await session.delete(url, headers=headers, json=payload)
            response.raise_for_status()
            data = await response.json()
            
            # Log response categories
            self.logger.info(f"VINs deactivated: {data.get('deenrollment-candidates', [])}")
            self.logger.info(f"VINs deactivated once confirmed: {data.get('scheduled-deenrollment-candidates', [])}")
            self.logger.info(f"VINs ignored because not activated or confirmed yet: {data.get('ignored-vins', [])}")

            return data

        except aiohttp.ClientResponseError as e:
            self.logger.error(f"Vehicle deactivation request failed ({e.status}): {e.message}")
            return {"error": f"Request failed with status {e.status}"}
        except Exception as e:
            self.logger.error(f"Unexpected error during vehicle deactivation: {str(e)}")
            return {"error": str(e)}
