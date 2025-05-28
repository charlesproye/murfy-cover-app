from datetime import datetime
from functools import lru_cache
from typing import Annotated, Sequence
from fastapi import Depends
import msgspec
from .schemas import BaseModelWithVin
from src.core.s3_utils import S3Dep, S3Service


class ResponseStorage:
    def __init__(self, s3: S3Service | None = None) -> None:
        self._s3 = s3 or S3Service()

    def store_basemodels_with_vin(self, objects: Sequence[BaseModelWithVin]):
        timestamp = int(datetime.now().timestamp() * 10e6)
        for object in objects:
            vin = object.vin
            filename = f"response/volkswagen/{vin}/temp/{timestamp}.json"
            encoded = msgspec.json.encode(object.model_dump_json())
            self._s3.store_object(encoded, filename)


@lru_cache
def get_response_storage(s3: S3Dep):
    return ResponseStorage(s3)


ResponseStorageDep = Annotated[ResponseStorage, Depends(get_response_storage)]

