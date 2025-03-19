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
from .schema import CarState, MergedCarState

# Configure botocore logger to be less verbose
logging.getLogger('botocore.httpchecksum').setLevel(logging.WARNING)

class MobilisightsCompresser:
    __logger: logging.Logger
    __s3: boto3.client
    __s3_dev: boto3.client
    __bucket: str
    __dev_bucket: str
    __s3_keys_by_vin: dict[str, dict[str, set[str]]] = {}
    __shutdown_requested: threading.Event
    __s3_config: Dict[str, str]
    __s3_dev_config: Dict[str, str]
    __upload_semaphore: asyncio.Semaphore  # Semaphore pour contrôler les uploads concurrents

    def __init__(self, threaded: bool = True, max_workers: int = 8):
        """Initialize the compressor with S3 credentials and configuration"""
        self.__logger = logging.getLogger("COMPRESSER")
        self.__shutdown_requested = threading.Event()
        self.__upload_semaphore = asyncio.Semaphore(3)  # Limite à 3 uploads simultanés
        
        # Load environment variables
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
        self.max_workers = max_workers

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
        """List all objects in S3 bucket for Stellantis"""
        self.__logger.info("Starting to list objects from S3...")
        try:
            paginator = self.__s3.get_paginator("list_objects_v2")
            brand_name = "stellantis"
            
            # Use delimiter to list only VINs first
            prefix = f"response/{brand_name.lower()}/"
            self.__logger.info(f"Listing VINs with prefix: {prefix}")
            
            vins = set()
            for page in paginator.paginate(
                Bucket=self.__bucket,
                Prefix=prefix,
                Delimiter='/'
            ):
                if 'CommonPrefixes' in page:
                    for prefix_obj in page['CommonPrefixes']:
                        vin = prefix_obj['Prefix'].split('/')[2]
                        if len(vin) == 17:  # Valid VIN length
                            vins.add(vin)
            
            self.__logger.info(f"Found {len(vins)} unique VINs")
            
            # Initialize the dictionary for the brand
            self.__s3_keys_by_vin[brand_name.lower()] = {}
            
            # Process VINs in batches
            batch_size = 30
            vin_batches = [list(vins)[i:i + batch_size] for i in range(0, len(vins), batch_size)]
            
            for batch_idx, vin_batch in enumerate(vin_batches, 1):
                self.__logger.info(f"Processing VIN batch {batch_idx}/{len(vin_batches)}")
                
                for vin in vin_batch:
                    if self.__shutdown_requested.is_set():
                        return
                        
                    temp_prefix = f"response/{brand_name.lower()}/{vin}/temp/"
                    temp_files = set()
                    
                    try:
                        for page in paginator.paginate(
                            Bucket=self.__bucket,
                            Prefix=temp_prefix,
                            PaginationConfig={'PageSize': 1000}
                        ):
                            if 'Contents' in page:
                                temp_files.update(obj["Key"] for obj in page["Contents"])
                        
                        if temp_files:
                            self.__s3_keys_by_vin[brand_name.lower()][vin] = temp_files
                            self.__logger.info(f"Found {len(temp_files)} temp files for VIN {vin}")
                        
                    except Exception as e:
                        self.__logger.error(f"Error listing temp files for VIN {vin}: {e}")
                        continue
                
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

    async def __process_batch_async(self, batch: List[str], vin: str, date: str):
        """Process a batch of files asynchronously"""
        session = aioboto3.Session()
        
        async with session.client(
            "s3",
            region_name=self.__s3_config['region'],
            endpoint_url=self.__s3_config['endpoint'],
            aws_access_key_id=self.__s3_config['key'],
            aws_secret_access_key=self.__s3_config['secret']
        ) as s3, session.client(
            "s3",
            region_name=self.__s3_dev_config['region'],
            endpoint_url=self.__s3_dev_config['endpoint'],
            aws_access_key_id=self.__s3_dev_config['key'],
            aws_secret_access_key=self.__s3_dev_config['secret']
        ) as s3_dev:
            car_states = []
            successful_processes = []
            
            # Process all files in batch
            tasks = []
            for s3_key in batch:
                if self.__shutdown_requested.is_set():
                    return [], None
                
                task = asyncio.create_task(self.__process_one_async(s3, s3_key))
                tasks.append((s3_key, task))

            for s3_key, task in tasks:
                try:
                    car_state = await task
                    if car_state:
                        car_states.append(car_state)
                        successful_processes.append(s3_key)
                except Exception as e:
                    self.__logger.error(f"Error processing {s3_key}: {e}")

            # Merge and upload if we have states
            if car_states:
                merged = MergedCarState.from_list(car_states)
                if merged:
                    try:
                        encoded = msgspec.json.encode(merged)
                        compressed_key = f"response/stellantis/{vin}/{date}.json"
                        
                        # Upload to main bucket first
                        await s3.put_object(
                            Bucket=self.__bucket,
                            Key=compressed_key,
                            Body=encoded
                        )
                        
                        # Upload to dev bucket with retry and semaphore control
                        success = await self.__upload_to_dev_bucket(s3_dev, compressed_key, encoded, vin)
                        
                        if success:
                            # Delete processed files in a single batch operation from main bucket only
                            if successful_processes:
                                try:
                                    delete_objects = {
                                        'Objects': [{'Key': key} for key in successful_processes],
                                        'Quiet': True
                                    }
                                    await s3.delete_objects(Bucket=self.__bucket, Delete=delete_objects)
                                    self.__logger.info(f"Successfully deleted {len(successful_processes)} temp files from main bucket for VIN {vin}")
                                except Exception as e:
                                    self.__logger.error(f"Error deleting temp files for VIN {vin}: {e}")
                            
                            self.__logger.info(f"Processed and merged {len(batch)} files for VIN {vin} on date {date}")
                            return successful_processes, merged
                        
                    except Exception as e:
                        self.__logger.error(f"Error uploading for VIN {vin} on date {date}: {e}")
                        
            return [], None

    async def __process_one_async(self, s3, s3_key: str):
        """Process a single file asynchronously"""
        try:
            response = await s3.get_object(Bucket=self.__bucket, Key=s3_key)
            async with response['Body'] as stream:
                content = await stream.read()
            
            return msgspec.json.decode(content, type=CarState)
                
        except Exception as e:
            self.__logger.error(f"Error processing {s3_key}: {e}")
            return None

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

    async def process_vin(self, vin: str, temp_data: set[str]):
        """Process all files for a single VIN"""
        if not temp_data:
            return

        try:
            # Group files by date
            files_by_date = self._group_files_by_date(temp_data)
            
            # Process each date separately
            for date, date_files in files_by_date.items():
                if self.__shutdown_requested.is_set():
                    return

                # Process in batches
                batch_size = 50
                date_files_list = list(date_files)
                batches = [date_files_list[i:i + batch_size] 
                          for i in range(0, len(date_files_list), batch_size)]
                
                # Process batches concurrently
                tasks = []
                for batch in batches:
                    if self.__shutdown_requested.is_set():
                        break
                        
                    task = asyncio.create_task(
                        self.__process_batch_async(batch, vin, date)
                    )
                    tasks.append(task)

                # Wait for all batch processing to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                successful_batches = 0
                for result in results:
                    if isinstance(result, tuple) and result[0]:
                        successful_batches += 1
                
                self.__logger.info(f"Completed processing for VIN {vin} on date {date}: {successful_batches}/{len(batches)} batches successful")
                
        except Exception as e:
            self.__logger.error(f"Error in process_vin for {vin}: {e}")

    async def run_async(self):
        """Run the compression process asynchronously"""
        self.list_objects()
        self.__logger.info("Starting Stellantis data compression")
        
        brand_name = "stellantis"
        if self.__s3_keys_by_vin.get(brand_name):
            # Process VINs concurrently with a limit
            max_concurrent_vins = 5
            vin_items = list(self.__s3_keys_by_vin[brand_name].items())
            
            for i in range(0, len(vin_items), max_concurrent_vins):
                batch = vin_items[i:i + max_concurrent_vins]
                tasks = []
                
                for vin, temp_data in batch:
                    if self.__shutdown_requested.is_set():
                        break
                        
                    task = asyncio.create_task(self.process_vin(vin, temp_data))
                    tasks.append((vin, task))
                
                for vin, task in tasks:
                    try:
                        await task
                        self.__logger.info(f"Completed processing for VIN {vin}")
                    except Exception as e:
                        self.__logger.error(f"Error processing VIN {vin}: {e}")
            
            self.__logger.info("Completed Stellantis data compression")
        else:
            self.__logger.warning("No data to compress for Stellantis")

    def run(self):
        """Main entry point to run the compression process"""
        asyncio.run(self.run_async())
