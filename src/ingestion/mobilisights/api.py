from datetime import datetime, timedelta, timezone
from hashlib import blake2s
from urllib.parse import quote, urlencode

import requests


class MSApi:
    __base_url: str
    __access_token: str
    __fleet_id: str
    __company: str

    def __init__(
        self, base_url: str, access_token: str, fleet_id: str, company: str
    ) -> None:
        self.__base_url = base_url
        self.__access_token = access_token
        self.__fleet_id = fleet_id
        self.__company = company

    def __format_datetime(self, datetime: datetime) -> str:
        return f"{datetime.year:02d}-{datetime.month:02d}-{datetime.day:02d}T{datetime.hour:02d}:{datetime.minute:02d}:{datetime.second:02d}.{datetime.microsecond/1000:03.0f}Z"

    def create_contract(
        self, vin: str, immat: str, duration: timedelta = timedelta(days=180)
    ):
        # Generate a 24 alphanumeric characters id
        id = blake2s(vin.encode(), digest_size=12).hexdigest()
        now = datetime.now(timezone.utc)
        now_fmt = self.__format_datetime(now)
        to_fmt = self.__format_datetime(now + duration)
        result = requests.post(
            f"{self.__base_url}/contracts",
            headers={"Authorization": f"Bearer {self.__access_token}"},
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

    def export_car_info(self):
        fleet_filter = {"fleets": [self.__fleet_id]}
        encoded_filter = urlencode(
            {"conditions": fleet_filter}, quote_via=quote
        ).replace("%27", "%22")
        result = requests.get(
            f"{self.__base_url}/exports/car-states.json?{encoded_filter}",
            params={"v": 2},
            headers={"Authorization": f"Bearer {self.__access_token}"},
            stream=True,
        )
        return result.status_code, result.iter_lines()

