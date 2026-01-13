from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from hashlib import blake2s

import requests


class MSApi:
    __base_url: str
    __email: str
    __password: str
    __company: str

    __auth_api_route = "api"
    __fleet_api_route = "connected-fleet/api"

    __token: str | None = None
    __token_exp = float("inf")

    def __init__(self, base_url: str, email: str, password: str, company: str) -> None:
        self.__base_url = base_url
        self.__email = email
        self.__password = password
        self.__company = company
        self.__fetch_token()

    def __format_datetime(self, datetime: datetime) -> str:
        return f"{datetime.year:02d}-{datetime.month:02d}-{datetime.day:02d}T{datetime.hour:02d}:{datetime.minute:02d}:{datetime.second:02d}.{datetime.microsecond / 1000:03.0f}Z"

    def __fetch_token(self):
        r = requests.post(
            f"{self.__base_url}/{self.__auth_api_route}/auth/login",
            data={"email": self.__email, "password": self.__password},
        ).json()
        self.__token = r.get("authToken")
        timestamp = (
            datetime.now(tz=UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        ).total_seconds()
        expires_in = 14 * 24 * 60 * 60  # 14 jours
        self.__token_exp = timestamp + expires_in

    def __get_token(self):
        timestamp = (
            datetime.now(tz=UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        ).total_seconds()
        if timestamp > self.__token_exp:
            self.__fetch_token()
        return self.__token

    def create_contract(
        self, vin: str, immat: str, duration: timedelta = timedelta(days=180)
    ):
        # Generate a 24 alphanumeric characters id
        id = blake2s(vin.encode(), digest_size=12).hexdigest()
        now = datetime.now(UTC)
        now_fmt = self.__format_datetime(now)
        to_fmt = self.__format_datetime(now + duration)
        token = self.__get_token()
        result = requests.post(
            f"{self.__base_url}/{self.__fleet_api_route}/contracts",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "reference": id,
                "company": self.__company,
                "car": {"vin": vin, "note": immat},
                "from": now_fmt,
                "to": to_fmt,
                "pack": "pack-1",
            },
        )
        return result.status_code, result.json()

    def export_car_info(self) -> tuple[int, Iterator[bytes]]:
        token = self.__get_token()
        result = requests.get(
            f"{self.__base_url}/{self.__fleet_api_route}/exports/car-states.json",
            params={"v": 2},
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
        )
        return result.status_code, result.iter_lines()
