import concurrent.futures
import logging
from collections.abc import Callable
from datetime import datetime
from queue import Queue
from typing import Optional

import boto3
import msgspec
from botocore.client import ClientError
from botocore.credentials import threading
from ingestion.bmw.multithreading import MergedInfoWrapper
from ingestion.bmw.schema import BMWInfo, BMWMergedInfo

class BMWCompresser:
    __logger: logging.Logger

    __s3 = boto3.client("s3")

    __s3_keys_by_vin: dict[str, dict[str, set[str]]] = {}

    __shutdown_requested = threading.Event()

    threaded: bool = False
    max_workers: Optional[int] = 8

    def __init__(
        self, s3, bucket, threaded: bool = False, max_workers: Optional[int] = 8
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__s3 = s3
        self.__bucket = bucket
        self.threaded = threaded
        self.max_workers = max_workers or self.max_workers

    def shutdown(self):
        self.__logger.info("Shutting down compresser")
        self.__shutdown_requested.set()

    def list_objects(self):
        paginator = self.__s3.get_paginator("list_objects_v2")
        brand_name = "BMW"
        bucket_iterator = paginator.paginate(
            Bucket=self.__bucket, Prefix=f"response/{brand_name}"
        )
        s3_keys = set()
        for obj in bucket_iterator:
            self.__logger.debug(f'Paginator response: {obj}')
            if 'Contents' in obj:
                for contents in obj["Contents"]:
                    key = contents["Key"]
                    s3_keys.add(key)
            else:
                self.__logger.warning(f"No objects found with prefix 'response/{brand_name}'")
        
        if not s3_keys:
            self.__logger.warning(f"No S3 objects found for brand {brand_name}")
        else:
            self.__logger.info(f"Listed {len(s3_keys)} temporary S3 objects for brand {brand_name}")
        vins = set(
            filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], s3_keys))
        )
        self.__s3_keys_by_vin[brand_name] = {
            k: set(
                filter(
                    lambda e: e.startswith(f"response/{brand_name}/{k}/temp/"),
                    s3_keys,
                )
            )
            for k in vins
        }
        self.__logger.info(f"Grouped temporary S3 objects for brand {brand_name}")

    def __process(self, items):
        for vin, temp_data in items.items():
            if self.__shutdown_requested.is_set():
                return
            if not temp_data:
                self.__logger.info(f"No temp files for VIN: {vin}, skipping compression.")
                continue

            self.__logger.info(f"Processing VIN: {vin}, Number of temp files: {len(temp_data)}")
            merged = MergedInfoWrapper[BMWInfo, BMWMergedInfo](BMWMergedInfo)

            self.__logger.info(f"Compressing data for VIN {vin}")
            for s3_key in temp_data:
                self.__process_one(s3_key, merged)
                self.__logger.info(f"After processing {s3_key}, merged data points: {len(merged.info.data_points)}")
                self.__logger.info(merged.info.get_fusion_stats())

            bmw_info = merged.info.to_bmw_info()
            self.__logger.info(f"Final merged data points for VIN {vin}: {len(bmw_info.data)}")
            self.__logger.info(f"Final fusion stats: {merged.info.get_fusion_stats()}")
            encoded = msgspec.json.encode(bmw_info)

            try:
                put_response = self.__s3.put_object(
                    Bucket=self.__bucket,
                    Key=f"response/BMW/{vin}/{datetime.today().date()}.json",
                    Body=encoded,
                )
            except ClientError as e:
                self.__logger.error(f"Failed to upload compressed data for VIN {vin}: {e}")
                return
            match put_response["ResponseMetadata"]["HTTPStatusCode"]:
                case 200:
                    self.__logger.info(f"Uploaded compressed data for VIN {vin}")
                case _:
                    self.__logger.error(f"Failed to upload compressed data for VIN {vin}: {put_response['Error']['Message']}")

    def __process_one(self, s3_key: str, merged: MergedInfoWrapper):
        self.__logger.info(f"Processing data point {s3_key}")
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
                self.__logger.error(f"Failed to fetch temporary data {s3_key}: {get_response}")
                return
        try:
            parsed = msgspec.json.decode(content, type=BMWInfo)
            self.__logger.info(f"Parsed data points from {s3_key}: {len(parsed.data)}")
        except msgspec.ValidationError as e:
            self.__logger.error(f"Failed to parse temporary data {s3_key}: {e}")
            return
        if parsed is not None:
            if merged.info is None:
                merged.set_info(parsed)
                self.__logger.info(f"Set initial info from {s3_key}")
            else:
                merged.merge(parsed)
                self.__logger.info(f"Merged data from {s3_key}")
        try:
            delete_response = self.__s3.delete_object(Bucket=self.__bucket, Key=s3_key)
        except ClientError as e:
            self.__logger.error(f"Error deleting temporary datapoint {s3_key}: {e}")
            return
        match delete_response["ResponseMetadata"]["HTTPStatusCode"]:
            case 204:
                self.__logger.info(f"Deleted temporary datapoint {s3_key}")
            case _:
                self.__logger.error(f"Error deleting temporary datapoint {s3_key}: {delete_response['Error']['Message']}")
                return

    def run(self):
        self.__logger.info("Listing bucket objects")
        self.list_objects()
        if not self.__shutdown_requested.is_set():
            self.__logger.info(f"Starting BMW data compression")
            self.__process(self.__s3_keys_by_vin["BMW"])

