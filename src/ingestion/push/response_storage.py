from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated

import msgspec
from fastapi import Depends
from pydantic import BaseModel

from core.s3.async_s3 import AsyncS3, get_async_s3

from .schemas import BaseModelWithVin

AsyncS3Dep = Annotated[AsyncS3, Depends(get_async_s3)]


class ResponseStorage:
    def __init__(self, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()

    async def store_basemodels_with_vin(
        self,
        car_brand: str,
        objects: Sequence[BaseModelWithVin],
    ):
        # Use UTC timestamp with microsecond precision
        timestamp = int(datetime.now(UTC).timestamp() * 1e6)

        for object_ in objects:
            vin = object_.vin
            object_ = self._remove_timezone_from_model(object_)
            filename = f"response/{car_brand}/{vin}/temp/{timestamp}.json"
            encoded = msgspec.json.encode(object_.model_dump())
            await self._s3.upload_file(filename, encoded)

    async def store_raw_bytes(
        self,
        car_brand: str,
        raw_bytes: bytes,
    ):
        # Store raw bytes directly
        timestamp = int(datetime.now(UTC).timestamp() * 1e6)
        filename = f"response/{car_brand}/raw/{timestamp}.bin"
        await self._s3.upload_file(filename, raw_bytes)

    async def store_raw_json(
        self,
        car_brand: str,
        json_data: dict | list,
    ):
        # Store raw JSON without Pydantic validation
        timestamp = int(datetime.now(UTC).timestamp() * 1e6)

        # Add received_date directly without creating a copy
        received_date = datetime.now(UTC).isoformat()
        if isinstance(json_data, dict):
            json_data["received_date"] = received_date
            vin = json_data["header"]["vin"]
        elif isinstance(json_data, list):
            for item in json_data:
                if isinstance(item, dict):
                    vin = item["header"]["vin"]
                    item["received_date"] = received_date

        filename = f"response/{car_brand}/{vin}/temp/{timestamp}.json"
        encoded = msgspec.json.encode(json_data)
        await self._s3.upload_file(filename, encoded)

    def _remove_timezone_from_model(self, model: BaseModel):
        updated_data = {
            field: self._remove_tz(value) for field, value in model.model_dump().items()
        }
        return model.__class__(**updated_data)

    def _remove_tz(self, obj: object):
        """
        Recursively remove timezone information from datetimes, lists, dicts, and BaseModels.
        """
        if isinstance(obj, datetime):
            return obj.replace(tzinfo=None)
        elif isinstance(obj, list):
            for i in range(len(obj)):
                obj[i] = self._remove_tz(obj[i])
            return obj
        elif isinstance(obj, dict):
            for key in obj:
                obj[key] = self._remove_tz(obj[key])
            return obj
        return obj


def get_response_storage(s3: AsyncS3Dep) -> ResponseStorage:
    """
    Return a new ResponseStorage instance without caching to avoid memory retention.
    """
    return ResponseStorage(s3)


ResponseStorageDep = Annotated[ResponseStorage, Depends(get_response_storage)]

