from datetime import datetime
from functools import lru_cache
from typing import Annotated, Sequence
from fastapi import Depends
import msgspec
from .schemas import BaseModelWithVin
from core.s3.async_s3 import AsyncS3Dep, AsyncS3


class ResponseStorage:
    def __init__(self, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()

    async def store_basemodels_with_vin(
        self,
        car_brand: str,
        objects: Sequence[BaseModelWithVin],
    ):
        timestamp = int(datetime.now().timestamp() * 10e6)
        for object in objects:
            vin = object.vin
            filename = f"response/{car_brand}/{vin}/temp/{timestamp}.json"
            encoded = msgspec.json.encode(object.model_dump_json())
            await self._s3.upload_file(filename, encoded)


@lru_cache
def get_response_storage(s3: AsyncS3Dep):
    return ResponseStorage(s3)


ResponseStorageDep = Annotated[ResponseStorage, Depends(get_response_storage)]

