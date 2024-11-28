import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import boto3
import msgspec
from botocore.client import ClientError
from botocore.credentials import threading
from ingestion.mobilisights.schema import CarState, MergedCarState


class MobilisightsCompresser:
    __logger: logging.Logger

    __s3 = boto3.client("s3")

    __vehicles: dict[str, set[str]]

    threaded: bool = False
    max_workers: Optional[int] = 8

    def __init__(
        self, s3, bucket, threaded: bool = False, max_workers: Optional[int] = 8
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__s3 = s3
        self.__bucket = bucket
        self.__logger.info("Listing bucket objects")
        self.list_objects()
        self.threaded = threaded
        self.max_workers = max_workers or self.max_workers

    def list_objects(self):
        paginator = self.__s3.get_paginator("list_objects_v2")
        it = paginator.paginate(Bucket=self.__bucket, Prefix="response/stellantis")
        vehicles = set()
        for obj in it:
            for con in obj["Contents"]:
                key = con["Key"]
                vehicles.add(key)
        vins = set(
            filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], vehicles))
        )
        self.__vehicles = {
            k: set(
                filter(
                    lambda e: e.startswith(f"response/stellantis/{k}/temp/"), vehicles
                )
            )
            for k in vins
        }
        self.__logger.info("Listed all vehicles")

    def __process_temp_data(
        self, s3_key: str, acc: list[CarState], acc_lock: threading.Lock
    ):
        try:
            get_response = self.__s3.get_object(Bucket=self.__bucket, Key=s3_key)
        except ClientError as e:
            self.__logger.error(f"Failed to fetch temporary data {s3_key}: {e}")
            return
        match get_response["ResponseMetadata"]["HTTPStatusCode"]:
            case 200:
                self.__logger.info(f"Fetched temporary data {s3_key} successfully")
                content = get_response["Body"].read().decode("utf-8")
            case _:
                self.__logger.error(
                    f"Failed to fetch temporary data {s3_key}: {get_response['Error']['Message']}"
                )
                return
        try:
            parsed = msgspec.json.decode(content, type=CarState)
            with acc_lock:
                acc.append(parsed)
        except (msgspec.ValidationError, msgspec.DecodeError) as e:
            self.__logger.error(f"Failed to parse temporary data {s3_key} : {e}")
            return
        try:
            delete_response = self.__s3.delete_object(Bucket=self.__bucket, Key=s3_key)
        except ClientError as e:
            self.__logger.error(f"Error deleting temporary datapoint {s3_key}: {e}")
            return
        match delete_response["ResponseMetadata"]["HTTPStatusCode"]:
            case 204:
                self.__logger.info(f"Deleted temporary datapoint {s3_key}")
            case _:
                self.__logger.error(
                    f"Error deleting temporary datapoint {s3_key}: {delete_response['Error']['Message']}"
                )
                return

    def __process_vin(self, vin, temp_data):
        today = datetime.today().date()
        self.__logger.info(f"Compressing data for VIN {vin}")
        lock = threading.Lock()
        temp_car_states = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as tp:
            for s3_key in temp_data:
                tp.submit(self.__process_temp_data, s3_key, temp_car_states, lock)
        try:
            merged = MergedCarState.from_list(temp_car_states)
            encoded = msgspec.json.encode(merged)
        except Exception as e:
            self.__logger.error(
                f"Failed to encode merged vehicle data for VIN {vin}: {e}"
            )
            return
        try:
            put_response = self.__s3.put_object(
                Bucket=self.__bucket,
                Key=f"response/stellantis/{vin}/{today}.json",
                Body=encoded,
            )
        except ClientError as e:
            self.__logger.error(
                f"Failed to upload compressed data for VIN {vin}: {e.response['Error']['Message']}"
            )
            return
        match put_response["ResponseMetadata"]["HTTPStatusCode"]:
            case 200:
                self.__logger.info(
                    f"Uploaded compressed data for VIN {vin} at location response/stellantis/{vin}/{today}.json"
                )
            case _:
                self.__logger.error(
                    f"Failed to upload compressed data for VIN {vin}: {put_response}"
                )
                return

    def compress(self):
        for vin, temp_data in self.__vehicles.items():
            self.__process_vin(vin, temp_data)

    def run(self):
        self.__logger.info("Starting renault data compression")
        self.compress()
        self.__logger.info("Compressed Stellantis vehicle data")

