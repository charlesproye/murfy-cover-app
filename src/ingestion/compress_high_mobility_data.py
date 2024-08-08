import logging
from datetime import datetime
from typing import Optional, TypeVar

import boto3
import msgspec
from ingestion.schema.high_mobility_schema import (
    KiaInfo,
    MercedesBenzInfo,
    MergedKiaInfo,
    MergedMercedesBenzInfo,
    MergedRenaultInfo,
    RenaultInfo,
)

T = TypeVar("T")


class HMCompresser:
    __s3 = boto3.client("s3")

    __kia: dict[str, set[str]]
    __renault: dict[str, set[str]]
    __mercedes_benz: dict[str, set[str]]

    def __init__(self, s3, bucket) -> None:
        self.__s3 = s3
        self.__bucket = bucket
        self.__list_objects()

    def __list_objects(self):
        paginator = self.__s3.get_paginator("list_objects_v2")
        it = paginator.paginate(Bucket=self.__bucket, Prefix="response")
        kia = set()
        renault = set()
        mercedes_benz = set()
        for obj in it:
            for con in obj["Contents"]:
                key = con["Key"]
                match key.split("/")[1]:
                    case "kia":
                        kia.add(key)
                    case "mercedes-benz":
                        mercedes_benz.add(key)
                    case "renault":
                        renault.add(key)
        kia_vins = set(map(lambda v: v.split("/")[2], kia))
        self.__kia = {
            k: set(filter(lambda e: e.startswith(f"response/kia/{k}/temp/"), kia))
            for k in kia_vins
        }
        renault_vins = set(map(lambda v: v.split("/")[2], renault))
        self.__renault = {
            k: set(
                filter(lambda e: e.startswith(f"response/renault/{k}/temp/"), renault)
            )
            for k in renault_vins
        }
        mercedes_benz_vins = set(map(lambda v: v.split("/")[2], mercedes_benz))
        self.__mercedes_benz = {
            k: set(
                filter(
                    lambda e: e.startswith(f"response/mercedes-benz/{k}/temp/"),
                    mercedes_benz,
                )
            )
            for k in mercedes_benz_vins
        }

    def compress_kia(self):
        def merge(ks: set[str]):
            merged: Optional[MergedKiaInfo] = None
            for k in ks:
                response = self.__s3.get_object(Bucket=self.__bucket, Key=k)
                content = response["Body"].read().decode("utf-8")
                parsed = msgspec.json.decode(content, type=KiaInfo)
                if parsed is not None:
                    if merged is None:
                        merged = MergedKiaInfo.from_initial(parsed)
                    else:
                        merged.merge(parsed)
            return merged

        for k, v in self.__kia.items():
            today = datetime.today()
            encoded = msgspec.json.encode(merge(v))
            self.__s3.put_object(
                Bucket=self.__bucket, Key=f"response/kia/{k}/{today}", Body=encoded
            )
            logging.info(f"Uploaded compressed data for VIN {k} (brand kia)")

    def compress_renault(self):
        def merge(ks: set[str]):
            merged: Optional[MergedRenaultInfo] = None
            for k in ks:
                response = self.__s3.get_object(Bucket=self.__bucket, Key=k)
                content = response["Body"].read().decode("utf-8")
                parsed = msgspec.json.decode(content, type=RenaultInfo)
                if parsed is not None:
                    if merged is None:
                        merged = MergedRenaultInfo.from_initial(parsed)
                    else:
                        merged.merge(parsed)
            return merged

        for k, v in self.__renault.items():
            today = datetime.today()
            encoded = msgspec.json.encode(merge(v))
            self.__s3.put_object(
                Bucket=self.__bucket, Key=f"response/renault/{k}/{today}", Body=encoded
            )
            logging.info(f"Uploaded compressed data for VIN {k} (brand renault)")

    def compress_mercedes_benz(self):
        def merge(ks: set[str]):
            merged: Optional[MergedMercedesBenzInfo] = None
            for k in ks:
                response = self.__s3.get_object(Bucket=self.__bucket, Key=k)
                content = response["Body"].read().decode("utf-8")
                parsed = msgspec.json.decode(content, type=MercedesBenzInfo)
                if parsed is not None:
                    if merged is None:
                        merged = MergedMercedesBenzInfo.from_initial(parsed)
                    else:
                        merged.merge(parsed)
            return merged

        for k, v in self.__mercedes_benz.items():
            today = datetime.today()
            encoded = msgspec.json.encode(merge(v))
            self.__s3.put_object(
                Bucket=self.__bucket, Key=f"response/renault/{k}/{today}", Body=encoded
            )
            logging.info(f"Uploaded compressed data for VIN {k} (brand mercedes-benz)")

    def run(self):
        self.compress_mercedes_benz()
        self.compress_kia()
        self.compress_renault()

