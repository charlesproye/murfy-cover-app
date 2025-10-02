from collections.abc import Sequence
from datetime import datetime
from functools import lru_cache
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
        timestamp = int(datetime.now().timestamp() * 10e6)
        for object_ in objects:
            vin = object_.vin
            object_ = self._remove_timezone_from_model(object_)
            filename = f"response/{car_brand}/{vin}/temp/{timestamp}.json"
            encoded = msgspec.json.encode(object_.model_dump_json())
            await self._s3.upload_file(filename, encoded)

    def _remove_timezone_from_model(self, model: BaseModel):
        updated_data = {
            field: self._remove_tz(value) for field, value in model.model_dump().items()
        }
        return model.__class__(**updated_data)

    def _remove_tz(self, obj: object):
        if isinstance(obj, datetime):
            return obj.replace(tzinfo=None)
        elif isinstance(obj, list):
            return [self._remove_tz(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._remove_tz(value) for key, value in obj.items()}
        elif isinstance(obj, BaseModel):
            return self._remove_timezone_from_model(obj)
        else:
            return obj

    def _add_received_date(self, model_dict: dict):
        model_dict["received_date"] = datetime.now()
        return model_dict


@lru_cache
def get_response_storage(s3: AsyncS3Dep):
    return ResponseStorage(s3)


ResponseStorageDep = Annotated[ResponseStorage, Depends(get_response_storage)]

