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
        logging.info("Listing bucket objects")
        self.list_objects()

    def list_objects(self):
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
        logging.info("Listed all kia vehicles")
        renault_vins = set(map(lambda v: v.split("/")[2], renault))
        self.__renault = {
            k: set(
                filter(lambda e: e.startswith(f"response/renault/{k}/temp/"), renault)
            )
            for k in renault_vins
        }
        logging.info("Listed all renault vehicles")
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
        logging.info("Listed all mercedes-benz vehicles")

    def __process_temp(self, items, brand, merge):
        for k, v in items.items():
            today = datetime.today().date()
            logging.info(f"Compressing data for VIN {k} (brand {brand})")
            try:
                encoded = msgspec.json.encode(merge(v))
            except msgspec.EncodeError as e:
                logging.error(
                    f"Failed to encode merged vehicle data for VIN {k} (brand {brand}): {e}"
                )
                return None
            put_response = self.__s3.put_object(
                Bucket=self.__bucket,
                Key=f"response/{brand}/{k}/{today}.json",
                Body=encoded,
            )
            match put_response["ResponseMetadata"]["HTTPStatusCode"]:
                case 200:
                    logging.info(
                        f"Uploaded compressed data for VIN {k} (brand {brand}) at location response/{brand}/{k}/{today}.json"
                    )
                case _:
                    logging.error(
                        f"Failed to upload compressed data for VIN {k} (brand {brand}): {put_response}"
                    )

    def compress_kia(self):
        def merge(ks: set[str]):
            merged: Optional[MergedKiaInfo] = None
            for k in ks:
                get_response = self.__s3.get_object(Bucket=self.__bucket, Key=k)
                match get_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 200:
                        logging.info(
                            f"Fetched temporary data {k} (brand kia) successfully"
                        )
                        content = get_response["Body"].read().decode("utf-8")
                    case _:
                        logging.error(
                            f"Failed to fetch temporary data {k} (brand kia): {get_response}"
                        )
                        return None
                try:
                    parsed = msgspec.json.decode(content, type=KiaInfo)
                except msgspec.ValidationError as e:
                    logging.error(
                        f"Failed to parse temporary data {k} (brand kia): {e}"
                    )
                    return None
                if parsed is not None:
                    if merged is None:
                        merged = MergedKiaInfo.from_initial(parsed)
                    else:
                        merged.merge(parsed)
                delete_response = self.__s3.delete_object(Bucket=self.__bucket, Key=k)
                match delete_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 204:
                        logging.info(f"Deleted temporary datapoint {k} (brand kia)")
                    case _:
                        logging.error(
                            f"Error deleting temporary datapoint {k} (brand kia): {delete_response}"
                        )
                        return None
            return merged

        self.__process_temp(self.__kia, "kia", merge)

    def compress_renault(self):
        def merge(ks: set[str]):
            merged: Optional[MergedRenaultInfo] = None
            for k in ks:
                get_response = self.__s3.get_object(Bucket=self.__bucket, Key=k)
                match get_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 200:
                        logging.info(
                            f"Fetched temporary data {k} (brand renault) successfully"
                        )
                        content = get_response["Body"].read().decode("utf-8")
                    case _:
                        logging.error(
                            f"Failed to fetch temporary data {k} (brand renault): {get_response}"
                        )
                        return None
                try:
                    parsed = msgspec.json.decode(content, type=RenaultInfo)
                except msgspec.ValidationError as e:
                    logging.error(
                        f"Failed to parse temporary data {k} (brand renault): {e}"
                    )
                    return None
                if parsed is not None:
                    if merged is None:
                        merged = MergedRenaultInfo.from_initial(parsed)
                    else:
                        merged.merge(parsed)
                delete_response = self.__s3.delete_object(Bucket=self.__bucket, Key=k)
                match delete_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 204:
                        logging.info(f"Deleted temporary datapoint {k} (brand renault)")
                    case _:
                        logging.error(
                            f"Error deleting temporary datapoint {k} (brand renault): {delete_response}"
                        )
                        return None
            return merged

        self.__process_temp(self.__renault, "renault", merge)

    def compress_mercedes_benz(self):
        def merge(ks: set[str]):
            merged: Optional[MergedMercedesBenzInfo] = None
            for k in ks:
                get_response = self.__s3.get_object(Bucket=self.__bucket, Key=k)
                match get_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 200:
                        logging.info(
                            f"Fetched temporary data {k} (brand mercedes-benz) successfully"
                        )
                        content = get_response["Body"].read().decode("utf-8")
                    case _:
                        logging.error(
                            f"Failed to fetch temporary data {k} (brand mercedes-benz): {get_response}"
                        )
                        return None
                try:
                    parsed = msgspec.json.decode(content, type=MercedesBenzInfo)
                except msgspec.ValidationError as e:
                    logging.error(
                        f"Failed to parse temporary data {k} (brand mercedes-benz): {e}"
                    )
                    return None
                if parsed is not None:
                    if merged is None:
                        merged = MergedMercedesBenzInfo.from_initial(parsed)
                    else:
                        merged.merge(parsed)
                delete_response = self.__s3.delete_object(Bucket=self.__bucket, Key=k)
                match delete_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 204:
                        logging.info(
                            f"Deleted temporary datapoint {k} (brand mercedes-benz)"
                        )
                    case _:
                        logging.error(
                            f"Error deleting temporary datapoint {k} (brand mercedes-benz): {delete_response}"
                        )
                        return None
            return merged

        self.__process_temp(self.__mercedes_benz, "mercedes-benz", merge)

    def run(self):
        logging.info("Starting mercedes-benz data compression")
        self.compress_mercedes_benz()
        logging.info("Starting kia data compression")
        self.compress_kia()
        logging.info("Starting renault data compression")
        self.compress_renault()

