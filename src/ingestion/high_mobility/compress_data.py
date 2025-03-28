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
import gc  # Import garbage collector


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
        """List S3 objects with memory-efficient pagination and per-brand processing"""
        self.__logger.info("Starting to list objects from S3 with memory-efficient approach")
        
        for brand_name in all_brands.keys():
            if self.__shutdown_requested.is_set():
                return
                
            self.__logger.info(f"Processing brand: {brand_name}")
            self.__s3_keys_by_vin[brand_name] = {}
            
            # First get a list of VINs only (not all keys at once)
            prefix = f"response/{brand_name}/"
            vins = set()
            
            try:
                paginator = self.__s3.get_paginator("list_objects_v2")
                
                # Use delimiter to list only common prefixes (directories/VINs)
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
                
                self.__logger.info(f"Found {len(vins)} unique VINs for brand {brand_name}")
                
                # Process VINs in smaller batches to avoid memory issues
                batch_size = 10
                vin_batches = [list(vins)[i:i + batch_size] for i in range(0, len(vins), batch_size)]
                
                for batch_idx, vin_batch in enumerate(vin_batches, 1):
                    self.__logger.info(f"Processing VIN batch {batch_idx}/{len(vin_batches)} for {brand_name}")
                    
                    for vin in vin_batch:
                        if self.__shutdown_requested.is_set():
                            return
                            
                        temp_prefix = f"response/{brand_name}/{vin}/temp/"
                        temp_files = set()
                        
                        try:
                            # Get temp files for this specific VIN only
                            for page in paginator.paginate(
                                Bucket=self.__bucket,
                                Prefix=temp_prefix,
                                PaginationConfig={'PageSize': 100}  # Smaller page size
                            ):
                                if 'Contents' in page:
                                    temp_files.update(obj["Key"] for obj in page["Contents"])
                            
                            if temp_files:
                                self.__s3_keys_by_vin[brand_name][vin] = temp_files
                                self.__logger.info(f"Found {len(temp_files)} temp files for VIN {vin}")
                            
                        except Exception as e:
                            self.__logger.error(f"Error listing temp files for VIN {vin}: {e}")
                            continue
                
            except Exception as e:
                self.__logger.error(f"Error listing objects for brand {brand_name}: {str(e)}")
                continue
            
            # Log summary for this brand
            if brand_name in self.__s3_keys_by_vin:
                total_vins = len(self.__s3_keys_by_vin[brand_name])
                total_files = sum(len(files) for files in self.__s3_keys_by_vin[brand_name].values())
                self.__logger.info(f"Total for {brand_name}: {total_vins} VINs with {total_files} temp files")
            else:
                self.__logger.warning(f"No data found for brand {brand_name}")
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
        """Process a single file with retry logic"""
        max_retries = 3
        base_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:  # Add delay before retries
                    await asyncio.sleep(base_delay * (2 ** attempt))  # Exponential backoff
                    self.__logger.info(f"Retry {attempt}/{max_retries} for processing {s3_key}")
                
                response = await s3.get_object(Bucket=self.__bucket, Key=s3_key)
                async with response['Body'] as stream:
                    content = await stream.read()
                    content_str = content.decode('utf-8')
                    
                # Parse with specific error handling
                try:
                    parsed = msgspec.json.decode(content_str, type=all_brands[brand].info_class)
                except ValueError as e:
                    self.__logger.error(f"JSON parsing error for {s3_key}: {e}")
                    return False
                except Exception as e:
                    self.__logger.error(f"Unexpected parsing error for {s3_key}: {e}")
                    return False
                
                if parsed is not None:
                    if merged.info is None:
                        merged.set_info(parsed)
                    else:
                        if isinstance(merged.info, all_brands[brand].merged_info_class):
                            merged.merge(parsed)
                        else:
                            self.__logger.error(f"Type mismatch in merge for {s3_key}")
                            return False
                    return True
                else:
                    self.__logger.warning(f"Parsed result is None for {s3_key}")
                    return False
                    
            except asyncio.TimeoutError:
                self.__logger.warning(f"Timeout processing {s3_key} (attempt {attempt+1}/{max_retries})")
                if attempt == max_retries - 1:
                    self.__logger.error(f"Failed to process {s3_key} after {max_retries} attempts: Timeout")
                    return False
            except Exception as e:
                if attempt < max_retries - 1:
                    self.__logger.warning(f"Error processing {s3_key} (attempt {attempt+1}/{max_retries}): {e}")
                else:
                    self.__logger.error(f"Error processing {s3_key} after {max_retries} attempts: {e}")
                    return False
        
        return False

    async def __process(self, items, brand: str):
        """Process data for a brand with memory optimizations"""
        self.__logger.info(f"Starting process_all for brand {brand} with {len(items)} VINs")
        
        # Process VINs in smaller batches to reduce memory pressure
        vin_batch_size = 5  # Smaller number of VINs to process at once
        vin_list = list(items.keys())
        vin_batches = [vin_list[i:i + vin_batch_size] for i in range(0, len(vin_list), vin_batch_size)]
        
        self.__logger.info(f"Processing {len(vin_batches)} VIN batches for {brand}")
        
        for vin_batch_idx, vin_batch in enumerate(vin_batches, 1):
            self.__logger.info(f"Processing VIN batch {vin_batch_idx}/{len(vin_batches)} for {brand}")
            
            for vin in vin_batch:
                if self.__shutdown_requested.is_set():
                    self.__logger.info("Shutdown requested, stopping processing")
                    return

                temp_data = items.get(vin, set())
                if not temp_data:
                    self.__logger.info(f"No temp data for VIN {vin}, skipping")
                    continue
                    
                self.__logger.info(f"Processing VIN {vin} with {len(temp_data)} files")
                info_type = all_brands[brand].info_class
                merged_type = all_brands[brand].merged_info_class
                yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
                
                # Smaller batch size for files
                batch_size = min(25, self.__batch_size)  # Reduced from 50 if that was the default
                temp_data_list = list(temp_data)
                batches = [temp_data_list[i:i + batch_size] 
                          for i in range(0, len(temp_data_list), batch_size)]
                
                self.__logger.info(f"Split data into {len(batches)} batches of size {batch_size}")
                
                # Process only a few batches at a time to control memory
                max_concurrent_batches = 2
                for i in range(0, len(batches), max_concurrent_batches):
                    if self.__shutdown_requested.is_set():
                        self.__logger.info("Shutdown requested during batch creation")
                        break
                        
                    current_batches = batches[i:i + max_concurrent_batches]
                    tasks = []
                    
                    for batch_index, batch in enumerate(current_batches):
                        batch_overall_index = i + batch_index + 1
                        self.__logger.debug(f"Creating task for batch {batch_overall_index}/{len(batches)}")
                        merged = MergedInfoWrapper[info_type, merged_type](merged_type)
                        task = asyncio.create_task(
                            self.__process_batch_async(batch, vin, brand, merged)
                        )
                        tasks.append(task)

                    # Wait for this small group of batch processing to complete
                    self.__logger.info(f"Waiting for {len(tasks)} batch tasks to complete")
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Handle results and save compressed data right away
                    successful_batches = 0
                    for result_index, result in enumerate(results):
                        if isinstance(result, Exception):
                            self.__logger.error(f"Batch {i + result_index + 1} failed with exception: {result}")
                            continue
                            
                        successful_processes, merged = result
                        if successful_processes and merged and merged.info is not None:
                            await self.__save_compressed_data(
                                successful_processes, merged, vin, brand, yesterday
                            )
                            successful_batches += 1
                        else:
                            self.__logger.warning(f"Batch {i + result_index + 1}/{len(batches)} had no successful processes or invalid merge result")
                    
                    # Explicitly release resources
                    for task in tasks:
                        task = None
                    results = None
                    gc.collect()  # Force garbage collection
                
                self.__logger.info(f"Completed processing VIN {vin}")
                gc.collect()  # Force garbage collection after each VIN is processed
            
            # Collect garbage after each VIN batch
            gc.collect()
            self.__logger.info(f"Completed VIN batch {vin_batch_idx}/{len(vin_batches)}")

        self.__logger.info(f"Completed process_all for brand {brand}")
        gc.collect()  # Final garbage collection

    async def __upload_to_dev_bucket(self, s3_dev, compressed_key: str, encoded: bytes, vin: str):
        """Upload to dev bucket with retry logic and semaphore control"""
        async with self.__upload_semaphore:  # Utiliser le sémaphore pour contrôler la concurrence
            max_retries = 3
            base_delay = 0.5
            
            # Create a new aioboto3 session for dev bucket upload
            session = aioboto3.Session()
            async with session.client(
                "s3",
                region_name=self.__s3_dev_config['region'],
                endpoint_url=self.__s3_dev_config['endpoint'],
                aws_access_key_id=self.__s3_dev_config['key'],
                aws_secret_access_key=self.__s3_dev_config['secret']
            ) as async_s3_dev:
                for attempt in range(max_retries):
                    try:
                        if attempt > 0:  # Add delay before retries
                            await asyncio.sleep(base_delay * (2 ** attempt))  # Exponential backoff
                            
                        await async_s3_dev.put_object(
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
                success = await self.__upload_to_dev_bucket(None, compressed_key, encoded, vin)
                
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
        """Run the compression process with timeout protection and resource cleanup"""
        import gc  # Ensure garbage collection is available
        
        # Force garbage collection before starting
        gc.collect()
        
        # Set a reasonable timeout for the entire operation
        async def run_with_timeout():
            try:
                await asyncio.wait_for(process_all_brands(), timeout=3600)  # 1 hour timeout
            except asyncio.TimeoutError:
                self.__logger.error("Compression process timed out after 1 hour, forcing termination")
            except Exception as e:
                self.__logger.error(f"Error in compression process: {e}")
            finally:
                # Final cleanup
                gc.collect()
                
        async def process_all_brands():
            self.list_objects()
            self.__logger.info("Listing bucket objects")
            
            # Create tasks for brands with throttling
            brand_tasks = []
            for brand_name in all_brands.keys():
                if self.__shutdown_requested.is_set():
                    break
                    
                self.__logger.info(f"Creating task for brand {brand_name}")
                task = asyncio.create_task(self.__process_brand(brand_name))
                brand_tasks.append((brand_name, task))
                # Small delay between starting tasks to reduce contention
                await asyncio.sleep(0.5)
            
            # Wait for all brand processing to complete
            self.__logger.info(f"Starting processing of {len(brand_tasks)} brands")
            for brand_name, task in brand_tasks:
                if self.__shutdown_requested.is_set():
                    break
                    
                try:
                    await task
                    self.__logger.info(f"Completed processing for brand {brand_name}")
                except Exception as e:
                    self.__logger.error(f"Error processing brand {brand_name}: {e}")
                
                # Force garbage collection after each brand
                gc.collect()
            
            self.__logger.info("All brands processing completed")

        # Run the async process
        self.__logger.info("Starting brand processing with resource management")
        asyncio.run(run_with_timeout())
        self.__logger.info("Finished all brand processing")
