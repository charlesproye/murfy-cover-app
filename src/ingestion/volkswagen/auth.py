from typing import Final
import httpx
from src.core.typing_utils import ensure_exists, now
from datetime import datetime, timedelta
from .settings import VolksWagenSettings


class VolksWagenAuth:
    CS_URL: Final[str] = "https://fapi.fleet-interface.com/v1/fdp"

    def __init__(self, settings: VolksWagenSettings | None = None) -> None:
        self._settings = settings or VolksWagenSettings()
        self._client = httpx.AsyncClient()
        self._token_value: str | None = None
        self._expiration_date: datetime = now()
        self._org_sub_id_value: str | None = None

    @property
    def token(self) -> str:
        if self._token_value is None or now() > self._expiration_date - timedelta(
            minutes=1
        ):
            return self.get_token()
        return self._token_value

    @property
    def org_sub_id(self) -> str:
        if self._org_sub_id_value is None:
            return self.get_organization_sub_id()
        return self._org_sub_id_value

    def get_token(self) -> str:
        response = httpx.post(
            url=f"{self.CS_URL}/login/third-party-system",
            json={
                "username": self._settings.VW_USERNAME,
                "password": self._settings.VW_PASSWORD,
            },
            headers={"accept": "application/json", "Content-Type": "application/json"},
        )
        if response.status_code != 200:
            raise Exception()
        data = response.json()
        self._expiration_date = datetime.fromisoformat(
            ensure_exists(data["expiration"])
        )
        self._token_value = ensure_exists(data["value"])
        return self._token_value

    def get_organization_sub_id(self) -> str:
        data = self.get_organization_sub_ids()
        if len(data) == 0:
            self._org_sub_id_value = ensure_exists(
                self.post_organization_sub_id()["id"]
            )
        else:
            self._org_sub_id_value = ensure_exists(data[0]["id"])
        return self._org_sub_id_value

    def get_organization_sub_ids(self) -> list[dict]:
        response = httpx.get(
            url=f"https://api.onebusinessid.com/comid/organisations/{self._settings.VW_ORGANIZATION_ID}/customers",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        if response.status_code != 200:
            raise Exception()
        return response.json()

    def post_organization_sub_id(
        self,
        company_name: str = "Test Company Bib",
        address: str | None = None,
        zip: str | None = None,
        further_address: str | None = None,
        city: str | None = None,
        vat_id: str | None = None,
        external_id: str | None = None,
    ) -> dict:
        json = {
            k: v
            for k, v in {
                "companyName": company_name,
                "address": address,
                "zip": zip,
                "furtherAddress": further_address,
                "city": city,
                "vatId": vat_id,
                "externalId": external_id,
            }.items()
            if v is not None
        }
        response = httpx.post(
            url=f"https://api.onebusinessid.com/comid/organisations/{self._settings.VW_ORGANIZATION_ID}/customers",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            json=json,
        )
        if response.status_code != 201:
            raise Exception()
        return response.json()

    def delete_organization_sub_ids(self, sub_id: str):
        response = httpx.request(
            method="DELETE",
            url=f"https://api.onebusinessid.com/comid/organisations/{self._settings.VW_ORGANIZATION_ID}/customers/{sub_id}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return response

