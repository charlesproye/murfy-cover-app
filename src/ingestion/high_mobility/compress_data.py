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
                if "Contents" in obj:
                    for contents in obj["Contents"]:
                        key = contents["Key"]
                        s3_keys.add(key)
                else:
                    self.__logger.warning(f"No contents found for brand {brand_name}.")
            
            self.__logger.info(f"Listed temporary S3 objects for brand {brand_name}")
            
            vins = set(
                filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], s3_keys))
            )
            
            if vins:
                self.__s3_keys_by_vin[brand_name] = {
                    k: set(
                        filter(
                            lambda e: e.startswith(f"response/{brand_name}/{k}/temp/"),
                            s3_keys,
                        )
                    )
                    for k in vins
                }
            else:
                self.__logger.warning(f"No VINs found for brand {brand_name}.")
                self.__s3_keys_by_vin[brand_name] = {}
                continue  # Ajoutez cette ligne pour éviter de continuer le traitement

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

            info_type = all_brands[brand].info_class
            merged_type = all_brands[brand].merged_info_class
            yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Process in smaller batches
            batch_size = 1000
            temp_data_list = list(temp_data)
            
            for i in range(0, len(temp_data_list), batch_size):
                if self.__shutdown_requested.is_set():
                    return

                batch = temp_data_list[i:i + batch_size]
                merged = MergedInfoWrapper[info_type, merged_type](merged_type)
                successful_processes = []

                self.__logger.info(f"Processing batch {i//batch_size + 1} for VIN {vin} (brand {brand})")
                
                # Process all files in batch
                for s3_key in batch:
                    try:
                        process_one(s3_key, merged)
                        successful_processes.append(s3_key)
                    except Exception as e:
                        self.__logger.error(f"Error processing {s3_key}: {e}")
                        continue

                # Skip if no data was processed successfully
                if not successful_processes:
                    self.__logger.warning(f"No data processed successfully in batch for VIN {vin}")
                    continue

                # Verify merged data is not empty
                if merged.info is None:
                    self.__logger.error(f"Empty merged data for VIN {vin}, batch {i//batch_size + 1}")
                    continue

                # Vérifier si les données sont vides en utilisant msgspec
                try:
                    encoded = msgspec.json.encode(merged.info)
                    decoded = msgspec.json.decode(encoded)
                    
                    # Vérifier si toutes les listes dans l'objet sont vides
                    is_empty = True
                    for value in decoded.values():
                        if isinstance(value, list) and value:
                            is_empty = False
                            break
                        elif isinstance(value, dict) and any(v for v in value.values() if isinstance(v, list) and v):
                            is_empty = False
                            break

                    if is_empty:
                        self.__logger.error(f"All data lists are empty for VIN {vin}, batch {i//batch_size + 1}")
                        continue

                except Exception as e:
                    self.__logger.error(f"Error checking data validity for VIN {vin}: {e}")
                    continue

                # Try to save compressed data
                try:
                    compressed_key = f"response/{brand}/{vin}/{yesterday}.json"
                    
                    put_response = self.__s3.put_object(
                        Bucket=self.__bucket,
                        Key=compressed_key,
                        Body=encoded,  # On utilise l'encoded déjà créé
                    )

                    if put_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                        self.__logger.info(
                            f"Successfully uploaded compressed batch {i//batch_size + 1} for VIN {vin}"
                        )
                        
                        # Only delete files that were successfully processed and after successful compression
                        for temp_key in successful_processes:
                            try:
                                delete_response = self.__s3.delete_object(
                                    Bucket=self.__bucket,
                                    Key=temp_key
                                )
                                if delete_response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                                    self.__logger.info(f"Deleted temporary file: {temp_key}")
                                else:
                                    self.__logger.warning(f"Failed to delete temporary file: {temp_key}")
                            except ClientError as e:
                                self.__logger.error(f"Error deleting temporary file {temp_key}: {e}")
                    else:
                        self.__logger.error(
                            f"Failed to upload compressed batch {i//batch_size + 1} for VIN {vin}: {put_response}"
                        )
                except Exception as e:
                    self.__logger.error(f"Error during compression for VIN {vin}, batch {i//batch_size + 1}: {e}")

    def run(self):
        self.list_objects()
        self.__logger.info("Listing bucket objects")
        for brand_name in all_brands.keys():
            if not self.__shutdown_requested.is_set():
                self.__logger.info(f"Starting {brand_name} data compression")
                if self.__s3_keys_by_vin[brand_name]:
                    self.__process(self.__s3_keys_by_vin[brand_name], brand_name)

