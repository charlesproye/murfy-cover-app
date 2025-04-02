import asyncio
import aioboto3
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from itertools import islice
import multiprocessing as mp
from datetime import datetime, timedelta
import time
import logging
import os
from botocore.client import Config
from typing import Optional, Dict
import boto3
import msgspec
from .schema import BMWInfo, BMWMergedInfo
from multithreading import MergedInfoWrapper
import botocore.config
import dotenv

class BMWCompresser:
    def __init__(
        self, 
        s3, 
        bucket, 
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
        self.batch_size = batch_size
        self.max_workers = max_workers or mp.cpu_count()
        
        # Configure boto3 to use signature version 4 with optimized settings
        self.__config = Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'},
            retries={'max_attempts': 2},  # Reduced retries
            max_pool_connections=25  # Increased connection pool
        )
        self.__session = aioboto3.Session()
        self.__s3_keys_by_vin = {}
        self.__shutdown_requested = asyncio.Event()

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

    def _get_date_from_filename(self, filename: str) -> str:
        """Extract date from timestamp filename"""
        try:
            timestamp = int(filename.split('/')[-1].split('.')[0])
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        except Exception as e:
            self.__logger.error(f"Error parsing date from filename {filename}: {e}")
            return None

    def _group_files_by_date(self, temp_files: set[str]) -> dict[str, set[str]]:
        """Group files by their date"""
        files_by_date = {}
        for file in temp_files:
            date = self._get_date_from_filename(file)
            if date:
                if date not in files_by_date:
                    files_by_date[date] = set()
                files_by_date[date].add(file)
        return files_by_date

    async def list_objects(self):
        self.__logger.info("Starting to list objects from S3...")
        try:
            async with self.__session.client(
                "s3",
                region_name=self.__s3.meta.region_name,
                aws_access_key_id=self.__s3._request_signer._credentials.access_key,
                aws_secret_access_key=self.__s3._request_signer._credentials.secret_key,
                endpoint_url=self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else None,
                config=self.__config
            ) as s3:
                self.__logger.info(f"S3 client created with endpoint: {self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else 'default'}")
                paginator = s3.get_paginator("list_objects_v2")
                brand_name = "bmw"
                s3_keys = set()
                
                prefix = f"response/{brand_name.lower()}"
                self.__logger.info(f"Listing objects with prefix: {prefix}")
                
                async for page in paginator.paginate(
                    Bucket=self.__bucket, 
                    Prefix=prefix
                ):
                    if 'Contents' in page:
                        s3_keys.update(obj["Key"] for obj in page["Contents"])
                
                self.__logger.info(f"Found {len(s3_keys)} total objects")
                
                # Process VINs in parallel
                vins = set(
                    filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], s3_keys))
                )
                
                self.__logger.info(f"Found {len(vins)} unique VINs")
                
                self.__s3_keys_by_vin[brand_name.lower()] = {
                    vin: set(filter(
                        lambda e: e.startswith(f"response/{brand_name.lower()}/{vin}/temp/"),
                        s3_keys
                    ))
                    for vin in vins
                }
                
                total_temp_files = sum(len(files) for files in self.__s3_keys_by_vin[brand_name.lower()].values())
                self.__logger.info(f"Found {total_temp_files} total temp files to process")
                
        except Exception as e:
            self.__logger.error(f"Error listing objects: {str(e)}", exc_info=True)
            raise

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
                        Body=encoded,
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

    async def __process_batch(self, vin: str, batch: list[str], date: str):
        async with self.__session.client(
            "s3",
            region_name=self.__s3_config['region'],
            endpoint_url=self.__s3_config['endpoint'],
            aws_access_key_id=self.__s3_config['key'],
            aws_secret_access_key=self.__s3_config['secret'],
            config=self.__config
        ) as s3, self.__session.client(
            "s3",
            region_name=self.__s3_dev_config['region'],
            endpoint_url=self.__s3_dev_config['endpoint'],
            aws_access_key_id=self.__s3_dev_config['key'],
            aws_secret_access_key=self.__s3_dev_config['secret'],
            config=self.__config
        ) as s3_dev:
            merged = MergedInfoWrapper[BMWInfo, BMWMergedInfo](BMWMergedInfo)
            
            # Fetch all objects in batch concurrently
            tasks = [
                self.__fetch_and_process(s3, s3_key, merged)
                for s3_key in batch
            ]
            await asyncio.gather(*tasks)
            
            # Upload merged batch
            if merged.info is not None:
                bmw_info = merged.info.to_bmw_info()
                encoded = msgspec.json.encode(bmw_info)
                compressed_key = f"response/bmw/{vin}/{date}.json"
                
                # Upload to main bucket first
                await s3.put_object(
                    Bucket=self.__bucket,
                    Key=compressed_key,
                    Body=encoded,
                )
                
                # Upload to dev bucket with retry and semaphore control
                success = await self.__upload_to_dev_bucket(s3_dev, compressed_key, encoded, vin)
                
                if success:
                    # Delete processed files in batch from main bucket only
                    try:
                        delete_objects = {'Objects': [{'Key': key} for key in batch]}
                        await s3.delete_objects(Bucket=self.__bucket, Delete=delete_objects)
                        self.__logger.info(f"Successfully deleted {len(batch)} temp files from main bucket for VIN {vin}")
                    except Exception as e:
                        self.__logger.error(f"Error deleting temp files for VIN {vin}: {e}")
                    
                    self.__logger.info(f"Processed and merged {len(batch)} files for VIN {vin} on date {date}")

    async def __fetch_and_process(self, s3, s3_key: str, merged: MergedInfoWrapper):
        try:
            response = await s3.get_object(Bucket=self.__bucket, Key=s3_key)
            async with response['Body'] as stream:
                content = await stream.read()
            
            parsed = msgspec.json.decode(content, type=BMWInfo)
            
            if merged.info is None:
                merged.set_info(parsed)
            else:
                merged.merge(parsed)
                
        except Exception as e:
            self.__logger.error(f"Error processing {s3_key}: {e}")

    async def process_vin(self, vin: str, temp_data: set[str]):
        if not temp_data:
            return

        try:
            # Group files by date
            files_by_date = self._group_files_by_date(temp_data)
            
            # Process each date separately
            for date, date_files in files_by_date.items():
                if self.__shutdown_requested.is_set():
                    return

                # Reduce batch size if there are too many files
                adjusted_batch_size = min(self.batch_size, 20)  # Limit max batch size
                
                # Process in smaller batches for each date
                batches = [
                    list(islice(date_files, i, i + adjusted_batch_size))
                    for i in range(0, len(date_files), adjusted_batch_size)
                ]
                
                # Process batches sequentially instead of all at once
                for batch in batches:
                    if self.__shutdown_requested.is_set():
                        return
                        
                    try:
                        await self.__process_batch(vin, batch, date)
                        # Add small delay between batches to prevent overload
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        self.__logger.error(f"Failed processing batch for VIN {vin} on date {date}: {e}")
                        continue  # Continue with next batch even if one fails
                
                self.__logger.info(f"Completed processing for VIN {vin} on date {date}")
                
        except Exception as e:
            self.__logger.error(f"Error in process_vin for {vin}: {e}")

    async def run(self):
        self.__logger.info("Starting bmw data compression")
        try:
            await self.list_objects()
            
            if self.__shutdown_requested.is_set():
                return
                
            # Process multiple VINs concurrently, but limit concurrency
            max_concurrent_vins = 5  # Ajuster selon les besoins
            tasks = []
            
            for vin, temp_data in self.__s3_keys_by_vin["bmw"].items():
                if self.__shutdown_requested.is_set():
                    break
                    
                tasks.append(self.process_vin(vin, temp_data))
                
                # When we reach max_concurrent_vins, wait for them to complete
                if len(tasks) >= max_concurrent_vins:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
            
            # Process any remaining tasks
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                    
            self.__logger.info("Completed bmw data compression")
        except Exception as e:
            self.__logger.error(f"Error in compression run: {e}")
            raise

    def start(self):
        asyncio.run(self.run())

    def shutdown(self):
        self.__shutdown_requested.set()
