from typing import Final, Literal
import httpx
from pydantic import BaseModel, Field
from .auth import VolksWagenAuth
from .schemas import VehicleConfirmation


class ConnectionApiKey(BaseModel):
    name: str
    value: str


class ConnectionAuthentication(BaseModel):
    type: Literal["BASIC_AUTH"]
    username: str
    password: str


class ConnectionUpdate(BaseModel):
    id: int
    type: Literal["WEBDAV", "WEBSERVICE"]
    url: str
    api_key: ConnectionApiKey | None = Field(default=None, alias="api-key")
    authentication: ConnectionAuthentication | None = None


class VolksWagenClient:
    FDE_URL: Final[str] = "https://fde.fleet-interface.com/v1/fdp"
    VEHICLE_URL: Final[str] = "https://vehicle-service.fleet-interface.com/v1/fdp"
    CS_URL: Final[str] = "https://fapi.fleet-interface.com/v1/fdp"

    def __init__(self) -> None:
        self._auth = VolksWagenAuth()
        self._client = httpx.AsyncClient()

    @property
    def _auth_headers(self):
        return {
            "Authorization": f"Bearer {self._auth.token}",
            "x-sub-organisation-id": f"{self._auth.org_sub_id}",
        }

    async def get_vehicles(self):
        response = await self._client.get(
            url=f"{self.FDE_URL}/vehicles",
            headers=self._auth_headers,
        )
        return response

    async def get_contexts(self) -> httpx.Response:
        response = await self._client.get(
            url=f"{self.FDE_URL}/contexts",
            headers=self._auth_headers,
        )
        return response

    async def get_vehicle_approval(self):
        response = await self._client.get(
            url=f"{self.VEHICLE_URL}/vehicle-approval",
            headers=self._auth_headers,
        )
        return response

    async def post_vehicle_approval(self, vins: list[str]):
        response = await self._client.post(
            url=f"{self.VEHICLE_URL}/vehicle-approval",
            headers=self._auth_headers,
            json={"vins": vins},
        )
        return response

    async def delete_vehicle_approval(self, vins: list[str]):
        response = await self._client.request(
            method="DELETE",
            url=f"{self.VEHICLE_URL}/vehicle-approval",
            headers=self._auth_headers,
            json={"vins": vins},
        )
        return response

    async def post_vehicle_confirmation(
        self,
        vehicle_confirmations: list[VehicleConfirmation],
    ):
        response = await self._client.post(
            url=f"{self.VEHICLE_URL}/vehicle-confirmation",
            headers=self._auth_headers,
            json={
                "vehicle-confirmations": [
                    {
                        "vin": vc.vin,
                        "verification-code": vc.verification_code,
                    }
                    for vc in vehicle_confirmations
                ]
            },
        )
        return response

    async def get_vehicle_status(
        self,
        vin: str | None = None,
        enrollment_state: str | None = None,
    ):
        params = None
        if vin is not None or enrollment_state is not None:
            params = {
                "vin": vin,
                "enrollment-state": enrollment_state,
            }
        response = await self._client.get(
            url=f"{self.VEHICLE_URL}/vehicle-status",
            headers=self._auth_headers,
            params=params,
        )
        return response

    async def get_bundles(self):
        response = await self._client.get(
            url=f"{self.CS_URL}/bundles",
            headers=self._auth_headers,
        )
        return response

    async def get_vehicle_privacy_modes(self, vin: str | None = None):
        path = f"/{vin}" if vin is not None else ""
        response = await self._client.get(
            url=f"{self.VEHICLE_URL}/vehicle-privacy-modes" + path,
            headers=self._auth_headers,
        )
        return response

    async def get_vehicles_attributes(
        self,
        vin: str,
        meta_data: bool = False,
    ):
        response = await self._client.get(
            url=f"{self.VEHICLE_URL}/vehicles/{vin}/attributes",
            headers=self._auth_headers,
            params={"meta-only": meta_data},
        )
        return response

    async def get_context_vehicles(self, context_id: int):
        response = await self._client.get(
            url=f"{self.FDE_URL}/context/{context_id}/vehicles",
            headers=self._auth_headers,
        )
        return response

    async def get_context_data(self, context_id: int):
        response = await self._client.get(
            url=f"{self.FDE_URL}/context/{context_id}/data",
            headers=self._auth_headers,
        )
        return response

    async def get_connections_status(self):
        response = await self._client.get(
            url=f"{self.CS_URL}/connections/status",
            headers=self._auth_headers,
        )
        return response

    async def put_connections(self, connection: ConnectionUpdate):
        response = await self._client.put(
            url=f"{self.CS_URL}/connections",
            headers=self._auth_headers,
            json=connection.model_dump(by_alias=True),
        )
        return response

    async def put_connections_status(self):
        response = await self._client.put(
            url=f"{self.CS_URL}/connections/status",
            headers=self._auth_headers,
        )
        return response

