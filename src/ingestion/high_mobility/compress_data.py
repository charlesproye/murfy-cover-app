import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import aioboto3
import boto3
import dotenv
import msgspec
from botocore.credentials import threading
from botocore.client import Config
from ingestion.high_mobility.multithreading import MergedInfoWrapper
from ingestion.high_mobility.schema import all_brands
import multiprocessing as mp


class HMCompresser:
    __logger: logging.Logger
    __s3: boto3.client
    __s3_dev: boto3.client
    __bucket: str
    __dev_bucket: str
    __s3_keys_by_vin: dict[str, dict[str, set[str]]] = {}
    __shutdown_requested: threading.Event
    __s3_config: Dict[str, str]
    __s3_dev_config: Dict[str, str]
    __config: Config
    __session: aioboto3.Session
    __batch_size: int
    __max_workers: int
    __upload_semaphore: asyncio.Semaphore

    def __init__(
        self, 
        # s3, 
        # bucket, 
        threaded: bool = True,
        max_workers: Optional[int] = None,
        batch_size: int = 50
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__upload_semaphore = asyncio.Semaphore(3)  # Limite à 3 uploads simultanés
        
        # Load environment variables for both prod and dev
        dotenv.load_dotenv()
        self.__s3_config = self.__load_s3_config("S3")
        self.__s3_dev_config = self.__load_s3_config("S3_DEV")
        if not self.__s3_config or not self.__s3_dev_config:
            raise ValueError("Missing required S3 configuration")

        # Initialize main S3 client with optimized config
        self.__s3 = boto3.client(
            "s3",
            region_name=self.__s3_config['region'],
            endpoint_url=self.__s3_config['endpoint'],
            aws_access_key_id=self.__s3_config['key'],
            aws_secret_access_key=self.__s3_config['secret'],
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'},
                retries={'max_attempts': 2},  # Reduced retries
                max_pool_connections=25  # Increased connection pool
            )
        )
        
        # Initialize dev S3 client with optimized config
        self.__s3_dev = boto3.client(
            "s3",
            region_name=self.__s3_dev_config['region'],
            endpoint_url=self.__s3_dev_config['endpoint'],
            aws_access_key_id=self.__s3_dev_config['key'],
            aws_secret_access_key=self.__s3_dev_config['secret'],
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'},
                retries={'max_attempts': 2},  # Reduced retries
                max_pool_connections=25  # Increased connection pool
            )
        )
        
        self.__bucket = self.__s3_config['bucket']
        self.__dev_bucket = self.__s3_dev_config['bucket']
        self.threaded = threaded
        self.__batch_size = batch_size
        self.__max_workers = max_workers or mp.cpu_count()
        
        # Configure boto3 to use signature version 4 with optimized settings
        self.__config = Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'},
            retries={'max_attempts': 2},  # Reduced retries
            max_pool_connections=25  # Increased connection pool
        )
        self.__session = aioboto3.Session()
        self.__s3_keys_by_vin = {}
        self.__shutdown_requested = threading.Event()

    def __load_s3_config(self, prefix: str) -> Optional[Dict[str, str]]:
        """Load S3 configuration from environment variables with given prefix"""
        if prefix == "S3_DEV":
            required_vars = {
                'endpoint': 'S3_ENDPOINT',
                'region': 'S3_REGION',
                'bucket': 'S3_BUCKET_DEV',
                'key': 'S3_KEY_DEV',
                'secret': 'S3_SECRET_DEV'
            }
        else:
            required_vars = {
                'endpoint': 'S3_ENDPOINT',
                'region': 'S3_REGION',
                'bucket': 'S3_BUCKET',
                'key': 'S3_KEY',
                'secret': 'S3_SECRET'
            }
        
        config = {}
        for key, env_var in required_vars.items():
            value = os.getenv(env_var)
            if value is None:
                self.__logger.error(f"{env_var} environment variable not found")
                return None
            config[key] = value
        
        return config

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

    async def __process_brand(self, brand_name: str):
        """Process a single brand asynchronously"""
        if not self.__shutdown_requested.is_set():
            self.__logger.info(f"Starting {brand_name} data compression")
            if self.__s3_keys_by_vin[brand_name]:
                await self.__process(self.__s3_keys_by_vin[brand_name], brand_name)
            else:
                self.__logger.warning(f"No data to compress for brand {brand_name}")

    async def __process_batch_async(self, batch: List[str], vin: str, brand: str, merged: MergedInfoWrapper):
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.__s3_config['region'],
            endpoint_url=self.__s3_config['endpoint'],
            aws_access_key_id=self.__s3_config['key'],
            aws_secret_access_key=self.__s3_config['secret']
        ) as s3:
            successful_processes = []
            tasks = []

            for s3_key in batch:
                if self.__shutdown_requested.is_set():
                    return [], None
                
                task = asyncio.create_task(self.__process_one_async(s3, s3_key, merged, brand))
                tasks.append((s3_key, task))

            for s3_key, task in tasks:
                try:
                    success = await task
                    if success:
                        successful_processes.append(s3_key)
                except Exception as e:
                    self.__logger.error(f"Error processing {s3_key}: {e}")

            return successful_processes, merged

    async def __process_one_async(self, s3, s3_key: str, merged: MergedInfoWrapper, brand: str):
        try:
            response = await s3.get_object(Bucket=self.__bucket, Key=s3_key)
            async with response['Body'] as stream:
                content = await stream.read()
                content = content.decode('utf-8')
                
            parsed = msgspec.json.decode(content, type=all_brands[brand].info_class)
            
            if parsed is not None:
                if merged.info is None:
                    merged.set_info(parsed)
                else:
                    if isinstance(merged.info, all_brands[brand].merged_info_class):
                        merged.merge(parsed)
                    else:
                        return False
                return True
        except Exception as e:
            self.__logger.error(f"Error in process_one_async for {s3_key}: {e}")
            return False

    async def __process(self, items, brand: str):
        self.__logger.info(f"Starting process_all for brand {brand} with {len(items)} VINs")
        
        for vin, temp_data in items.items():
            if self.__shutdown_requested.is_set():
                self.__logger.info("Shutdown requested, stopping processing")
                return

            self.__logger.info(f"Processing VIN {vin} with {len(temp_data)} files")
            info_type = all_brands[brand].info_class
            merged_type = all_brands[brand].merged_info_class
            yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Process in parallel batches
            batch_size = self.__batch_size
            temp_data_list = list(temp_data)
            batches = [temp_data_list[i:i + batch_size] 
                      for i in range(0, len(temp_data_list), batch_size)]
            
            self.__logger.info(f"Split data into {len(batches)} batches of size {batch_size}")
            
            # Process batches concurrently
            tasks = []
            for batch_index, batch in enumerate(batches):
                if self.__shutdown_requested.is_set():
                    self.__logger.info("Shutdown requested during batch creation")
                    break
                    
                self.__logger.debug(f"Creating task for batch {batch_index + 1}/{len(batches)}")
                merged = MergedInfoWrapper[info_type, merged_type](merged_type)
                task = asyncio.create_task(
                    self.__process_batch_async(batch, vin, brand, merged)
                )
                tasks.append(task)

            # Wait for all batch processing to complete
            self.__logger.info(f"Waiting for {len(tasks)} batch tasks to complete")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle results and save compressed data
            successful_batches = 0
            for result_index, (successful_processes, merged) in enumerate(results):
                if successful_processes and merged and merged.info is not None:
                    await self.__save_compressed_data(
                        successful_processes, merged, vin, brand, yesterday
                    )
                    successful_batches += 1
                else:
                    self.__logger.warning(f"Batch {result_index + 1}/{len(results)} had no successful processes or invalid merge result")
            
            self.__logger.info(f"Completed processing VIN {vin}: {successful_batches}/{len(batches)} batches processed successfully")

        self.__logger.info(f"Completed process_all for brand {brand}")

    async def __upload_to_dev_bucket(self, s3_dev, compressed_key: str, encoded: bytes, vin: str):
        """Upload to dev bucket with retry logic and semaphore control"""
        async with self.__upload_semaphore:  # Utiliser le sémaphore pour contrôler la concurrence
            max_retries = 3
            base_delay = 0.5
            
            for attempt in range(max_retries):
                try:
                    if attempt > 0:  # Add delay before retries
                        await asyncio.sleep(base_delay * (2 ** attempt))  # Exponential backoff
                        
                    await s3_dev.put_object(
                        Bucket=self.__dev_bucket,
                        Key=compressed_key,
                        Body=encoded
                    )
                    self.__logger.info(f"Successfully uploaded to dev bucket: {compressed_key}")
                    return True
                except Exception as e:
                    if "OperationAborted" in str(e) and attempt < max_retries - 1:
                        self.__logger.warning(f"Retry {attempt + 1}/{max_retries} for dev bucket upload: {compressed_key}")
                        continue
                    else:
                        self.__logger.error(f"Failed to upload to dev bucket: {compressed_key}, error: {e}")
                        return False

    async def __save_compressed_data(self, successful_processes, merged, vin, brand, yesterday):
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.__s3_config['region'],
            endpoint_url=self.__s3_config['endpoint'],
            aws_access_key_id=self.__s3_config['key'],
            aws_secret_access_key=self.__s3_config['secret']
        ) as s3:
            try:
                encoded = msgspec.json.encode(merged.info)
                compressed_key = f"response/{brand}/{vin}/{yesterday}.json"
                
                # Upload to main bucket first
                await s3.put_object(
                    Bucket=self.__bucket,
                    Key=compressed_key,
                    Body=encoded
                )

                # Upload to dev bucket with retry and semaphore control
                success = await self.__upload_to_dev_bucket(self.__s3_dev, compressed_key, encoded, vin)
                
                if success:
                    # Delete temp files from main bucket only
                    try:
                        delete_tasks = []
                        for temp_key in successful_processes:
                            task = asyncio.create_task(
                                s3.delete_object(Bucket=self.__bucket, Key=temp_key)
                            )
                            delete_tasks.append(task)
                        
                        await asyncio.gather(*delete_tasks)
                        self.__logger.info(f"Successfully deleted {len(successful_processes)} temp files from main bucket for VIN {vin}")
                    except Exception as e:
                        self.__logger.error(f"Error deleting temp files for VIN {vin}: {e}")
                
            except Exception as e:
                self.__logger.error(f"Error saving compressed data for VIN {vin}: {e}")

    def run(self):
        self.list_objects()
        self.__logger.info("Listing bucket objects")
        
        async def process_all_brands():
            # Create tasks for all brands
            brand_tasks = []
            for brand_name in all_brands.keys():
                self.__logger.info(f"Creating task for brand {brand_name}")
                task = asyncio.create_task(self.__process_brand(brand_name))
                brand_tasks.append((brand_name, task))
            
            # Wait for all brand processing to complete
            self.__logger.info(f"Starting parallel processing of {len(brand_tasks)} brands")
            for brand_name, task in brand_tasks:
                try:
                    await task
                    self.__logger.info(f"Completed processing for brand {brand_name}")
                except Exception as e:
                    self.__logger.error(f"Error processing brand {brand_name}: {e}")
            
            self.__logger.info("All brands processing completed")

        # Run all brands in parallel
        self.__logger.info("Starting parallel brand processing")
        asyncio.run(process_all_brands())
        self.__logger.info("Finished all brand processing")
