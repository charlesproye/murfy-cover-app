from datetime import datetime
from functools import lru_cache
from typing import Annotated, Any, Sequence

import boto3
from fastapi import Depends
import msgspec
from pydantic import BaseModel
from .schemas import BaseModelWithVin


class ResponseStorage:
    def __init__(self, s3_client=None) -> None:
        self.s3_client = s3_client or boto3.client("s3")
        self.bucket = "bucket"

    def store_object(self, object: bytes, filename: str):
        self.s3_client.put_object(
            Body=object,
            Bucket=self.bucket,
            Key=filename,
        )

    def store_basemodels_with_vin(self, objects: Sequence[BaseModelWithVin]):
        timestamp = int(datetime.now().timestamp() * 10e6)
        for object in objects:
            vin = object.vin
            filename = f"response/volkswagen/{vin}/temp/{timestamp}.json"
            encoded = msgspec.json.encode(object.model_dump_json())
            self.s3_client.put_object(
                Body=encoded,
                Bucket=self.bucket,
                Key=filename,
            )
        return


@lru_cache
def get_s3_client():
    return boto3.client("s3")


S3ClientDep = Annotated[Any, Depends(get_s3_client)]


@lru_cache
def get_response_storage():
    return ResponseStorage()


ResponseStorageDep = Annotated[ResponseStorage, Depends(get_response_storage)]

