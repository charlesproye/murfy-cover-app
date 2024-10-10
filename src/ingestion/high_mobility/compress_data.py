import concurrent.futures
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from queue import Queue
from typing import Optional

import boto3
import msgspec
from botocore.client import ClientError
from botocore.credentials import threading
from ingestion.high_mobility.multithreading import MergedInfoWrapper
from ingestion.high_mobility.schema import all_brands


class HMCompresser:
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
        for brand_name in all_brands.keys():
            bucket_iterator = paginator.paginate(
                Bucket=self.__bucket, Prefix=f"response/{brand_name}"
            )
            s3_keys = set()
            for obj in bucket_iterator:
                for contents in obj["Contents"]:
                    key = contents["Key"]
                    s3_keys.add(key)
            self.__logger.info(f"Listed temporary S3 objects for brand {brand_name}")
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

    def __process(self, items, brand: str):
        def process_one(s3_key: str, merged: MergedInfoWrapper):
            self.__logger.info(f"Processing data point {s3_key}")
            try:
                get_response = self.__s3.get_object(Bucket=self.__bucket, Key=s3_key)
            except ClientError as e:
                self.__logger.error(
                    f"Failed to fetch temporary data {s3_key} (brand {brand}): {e}"
                )
                return
            match get_response["ResponseMetadata"]["HTTPStatusCode"]:
                case 200:
                    self.__logger.info(
                        f"Fetched temporary data {s3_key} (brand {brand}) successfully"
                    )
                    content = get_response["Body"].read().decode("utf-8")
                case _:
                    self.__logger.error(
                        f"Failed to fetch temporary data {s3_key} (brand {brand}): {get_response}"
                    )
                    return
            try:
                parsed = msgspec.json.decode(content, type=all_brands[brand].info_class)
            except msgspec.ValidationError as e:
                self.__logger.error(
                    f"Failed to parse temporary data {s3_key} (brand {brand}): {e}"
                )
                return
            if parsed is not None:
                if merged.info is None:
                    merged.set_info(parsed)
                else:
                    if isinstance(merged.info, all_brands[brand].merged_info_class):
                        merged.merge(parsed)
                    else:
                        self.__logger.error(
                            "Cannot compress different vehicle types together"
                        )
                        return
        for vin, temp_data in items.items():
            if self.__shutdown_requested.is_set():
                return
            self.__logger.info(f"{vin}, {temp_data}")
            info_type = all_brands[brand].info_class
            merged_type = all_brands[brand].merged_info_class
            merged = MergedInfoWrapper[info_type, merged_type](merged_type)
            yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            job_queue: Queue[Callable] = Queue()

            self.__logger.info(f"Compressing data for VIN {vin} (brand {brand})")
            for s3_key in temp_data:
                job_queue.put(lambda s=s3_key, m=merged: process_one(s, m))
            self.__logger.info("starting thread pool")
            with concurrent.futures.ThreadPoolExecutor(self.max_workers) as e:
                while not job_queue.empty() and not self.__shutdown_requested.is_set():
                    job = job_queue.get()
                    e.submit(job)
                    job_queue.task_done()
            try:
                encoded = msgspec.json.encode(merged.info)
            except msgspec.EncodeError as e:
                self.__logger.error(
                    f"Failed to encode merged vehicle data for VIN {vin} (brand {brand}): {e}"
                )
                return
            try:
                put_response = self.__s3.put_object(
                    Bucket=self.__bucket,
                    Key=f"response/{brand}/{vin}/{yesterday}.json",
                    Body=encoded,
                )
            except ClientError as e:
                self.__logger.error(
                    f"Failed to upload compressed data for VIN {vin} (brand {brand}): {e}"
                )
                return
            if put_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                self.__logger.info(
                    f"Uploaded compressed data for VIN {vin} (brand {brand}) at location response/{brand}/{vin}/{yesterday}.json"
                )
                
                # Suppression de tous les fichiers temporaires pour ce VIN
                for temp_key in temp_data:
                    try:
                        delete_response = self.__s3.delete_object(
                            Bucket=self.__bucket, Key=temp_key
                        )
                        if delete_response['ResponseMetadata']['HTTPStatusCode'] == 204:
                            self.__logger.info(f"Deleted temporary file: {temp_key}")
                        else:
                            self.__logger.warning(f"Failed to delete temporary file: {temp_key}")
                    except ClientError as e:
                        self.__logger.error(f"Error deleting temporary file {temp_key}: {e}")
            else:
                self.__logger.error(
                    f"Failed to upload compressed data for VIN {vin} (brand {brand}): {put_response['Error']['Message']}"
                )
        else:
            self.__logger.info(f"VIN {vin} not found in the data for brand {brand}")

    def run(self):
        self.list_objects()
        self.__logger.info("Listing bucket objects")
        for brand_name in all_brands.keys():
            if not self.__shutdown_requested.is_set():
                self.__logger.info(f"Starting {brand_name} data compression")
                self.__process(self.__s3_keys_by_vin[brand_name], brand_name)
