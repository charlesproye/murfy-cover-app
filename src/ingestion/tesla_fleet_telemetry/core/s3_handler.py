import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
import random
from botocore.client import Config
from botocore.exceptions import ClientError

import aioboto3
import boto3

from src.ingestion.tesla_fleet_telemetry.config.settings import get_settings

# Silence the checksum validation messages
logging.getLogger('botocore.httpchecksum').setLevel(logging.WARNING)

logger = logging.getLogger("s3-handler")

# Global variables for S3 client cache
_s3_async_client = None
_s3_async_session = None
_s3_sync_client = None
# Cache of already compressed files to avoid reprocessing the same data
_compressed_files_cache: Dict[str, Set[str]] = {}


async def get_s3_async_client():
    """
    Returns an asynchronous S3 client, reusing it if it already exists.
    """
    global _s3_async_client, _s3_async_session
    
    if _s3_async_client is not None:
        return _s3_async_client
    
    settings = get_settings()
    _s3_async_session = aioboto3.Session()
    
    # Create a boto3 config that properly disables checksum validation
    boto_config = Config(
        signature_version='s3v4',
        s3={
            'addressing_style': 'path',
            'payload_signing_enabled': False,
            'use_accelerate_endpoint': False,
            'checksum_validation': False  # Disable checksum validation properly
        },
        connect_timeout=5,
        read_timeout=60,
        retries={'max_attempts': 3, 'mode': 'standard'}
    )
    
    # Enable boto3 debug logging for troubleshooting if needed
    # boto3.set_stream_logger('', logging.DEBUG)
    
    _s3_async_client = await _s3_async_session.client(
        's3',
        region_name=settings.s3_region,
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_key,
        aws_secret_access_key=settings.s3_secret,
        config=boto_config,
        verify=False  # Disable SSL verification if using a self-signed cert
    ).__aenter__()
    
    return _s3_async_client


def get_s3_sync_client():
    """
    Returns a synchronous S3 client, reusing it if it already exists.
    """
    global _s3_sync_client
    
    if _s3_sync_client is not None:
        return _s3_sync_client
    
    settings = get_settings()
    
    # Create a boto3 config that properly disables checksum validation
    boto_config = Config(
        signature_version='s3v4',
        s3={
            'addressing_style': 'path',
            'payload_signing_enabled': False,
            'use_accelerate_endpoint': False,
            'checksum_validation': False  # Disable checksum validation properly
        },
        connect_timeout=5,
        read_timeout=60,
        retries={'max_attempts': 3, 'mode': 'standard'}
    )
    
    _s3_sync_client = boto3.client(
        's3',
        region_name=settings.s3_region,
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_key,
        aws_secret_access_key=settings.s3_secret,
        config=boto_config,
        verify=False  # Disable SSL verification if using a self-signed cert
    )
    
    return _s3_sync_client


async def cleanup_clients():
    """
    Properly close all client connections when shutting down.
    """
    global _s3_async_client, _s3_sync_client, _s3_async_session
    
    logger.info("Cleaning up S3 client connections...")
    
    # Close the async client
    if _s3_async_client is not None:
        try:
            # We need to close the client context manager
            await _s3_async_client.__aexit__(None, None, None)
            _s3_async_client = None
            logger.debug("Async S3 client closed successfully")
        except Exception as e:
            logger.error(f"Error closing async S3 client: {str(e)}")
    
    # Close the sync client
    if _s3_sync_client is not None:
        try:
            _s3_sync_client.close()
            _s3_sync_client = None
            logger.debug("Sync S3 client closed successfully")
        except Exception as e:
            logger.error(f"Error closing sync S3 client: {str(e)}")
    
    # Clear the session
    _s3_async_session = None


async def save_data_to_s3(data: List[Dict[str, Any]], vin: str) -> bool:
    """
    Saves telemetry data to the S3 temp folder.
    
    Optimized to minimize S3 writes by grouping data by hour.
    
    Args:
        data: List of telemetry data to save
        vin: Vehicle VIN
        
    Returns:
        bool: True if save was successful, False otherwise
    """
    if not data or not vin:
        logger.warning("No data to save or missing VIN")
        return False
    
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    
    # Organize data by hour to group writes
    data_by_hour = {}
    for item in data:
        readable_date = item.get('readable_date', '')
        if readable_date:
            try:
                # Extract hour in YYYY-MM-DD_HH format
                hour_str = datetime.strptime(readable_date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d_%H")
                if hour_str not in data_by_hour:
                    data_by_hour[hour_str] = []
                data_by_hour[hour_str].append(item)
            except Exception:
                # Fallback if format is not as expected
                current_hour = datetime.now().strftime("%Y-%m-%d_%H")
                if current_hour not in data_by_hour:
                    data_by_hour[current_hour] = []
                data_by_hour[current_hour].append(item)
        else:
            # Use current hour if no readable date
            current_hour = datetime.now().strftime("%Y-%m-%d_%H")
            if current_hour not in data_by_hour:
                data_by_hour[current_hour] = []
            data_by_hour[current_hour].append(item)
    
    # Write one file per hour to reduce number of S3 operations
    success = True
    save_tasks = []
    
    for hour_str, hour_data in data_by_hour.items():
        key = f"{settings.base_s3_path}/{vin}/temp/{hour_str}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"
        save_tasks.append(save_object_to_s3(s3_client, bucket_name, key, hour_data))
    
    # Execute tasks in parallel
    if save_tasks:
        results = await asyncio.gather(*save_tasks, return_exceptions=True)
        success = all(result == True for result in results if not isinstance(result, Exception))
        
        if not success:
            errors = [str(result) for result in results if isinstance(result, Exception)]
            logger.error(f"Errors saving data: {errors}")
    
    return success


async def save_object_to_s3(s3_client, bucket_name: str, key: str, data: Any) -> bool:
    """
    Saves an object to S3 with retry on failure.
    
    Args:
        s3_client: aioboto3 S3 client
        bucket_name: S3 bucket name
        key: S3 key (file path)
        data: Data to save
        
    Returns:
        bool: True if save was successful, False otherwise
    """
    try:
        await s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.debug(f"Data successfully saved to {key}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to S3: {str(e)}")
        return False


async def compress_data() -> bool:
    """
    Compresses temporary data from all vehicles to optimize storage.
    Temp files are grouped by date and saved in a single file,
    then temporary files are deleted.
    
    Returns:
        bool: True if compression was successful, False otherwise
    """
    logger.info("Starting data compression")
    
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    base_path = settings.base_s3_path
    
    try:
        # Get list of vehicle prefixes
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{base_path}/",
            Delimiter="/"
        )
        
        if 'CommonPrefixes' not in response:
            logger.info("No vehicles found to compress")
            return True
            
        # Create compression tasks for each vehicle
        compression_tasks = []
        for prefix in response.get('CommonPrefixes', []):
            vehicle_prefix = prefix.get('Prefix', '')
            if vehicle_prefix:
                vin = vehicle_prefix.split('/')[-2]
                task = asyncio.create_task(compress_vehicle_data(s3_client, bucket_name, vin))
                compression_tasks.append(task)
        
        if not compression_tasks:
            logger.info("No compression tasks created")
            return True
            
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(50)  # Max 50 parallel tasks
        
        async def compress_with_semaphore(task):
            async with semaphore:
                return await task
                
        # Execute compression tasks
        results = await asyncio.gather(
            *[compress_with_semaphore(task) for task in compression_tasks],
            return_exceptions=True
        )
        
        # Check for errors
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        logger.info(f"Compression completed: {success_count} vehicles processed successfully, {error_count} errors")
        
        if error_count > 0:
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error compressing vehicle {i}: {str(result)}")
        
        # Clean compressed files cache once a day
        if datetime.now().hour == 4:  # At 4am
            _compressed_files_cache.clear()
            logger.info("Compressed files cache cleared")
        
        return error_count == 0
        
    except Exception as e:
        logger.error(f"Error during data compression: {str(e)}")
        return False


async def compress_vehicle_data(s3_client, bucket_name: str, vin: str) -> bool:
    """
    Compresses temporary data for a specific vehicle.
    
    Args:
        s3_client: aioboto3 S3 client
        bucket_name: S3 bucket name
        vin: Vehicle VIN
        
    Returns:
        bool: True if compression was successful, False otherwise
    """
    global _compressed_files_cache
    
    settings = get_settings()
    temp_folder = f"{settings.base_s3_path}/{vin}/temp/"
    
    try:
        # List temporary files
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=temp_folder
        )
        
        if 'Contents' not in response or not response['Contents']:
            logger.debug(f"No temporary files to compress for vehicle {vin}")
            return True
            
        # Initialize cache for this VIN if it doesn't exist
        if vin not in _compressed_files_cache:
            _compressed_files_cache[vin] = set()
        
        # Filter already processed files
        new_files = []
        for obj in response.get('Contents', []):
            file_key = obj['Key']
            if file_key not in _compressed_files_cache[vin]:
                new_files.append(obj)
        
        if not new_files:
            logger.debug(f"No new temporary files to compress for vehicle {vin}")
            return True
        
        logger.info(f"Compressing {len(new_files)} new files for vehicle {vin}")
        
        # Organize data by date
        data_by_date = {}
        files_to_delete = []
        
        for obj in new_files:
            try:
                file_key = obj['Key']
                file_response = await s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = await file_response['Body'].read()
                
                try:
                    file_data = json.loads(file_content)
                except json.JSONDecodeError:
                    logger.error(f"Corrupted JSON in file {file_key}, deleting...")
                    files_to_delete.append(file_key)
                    continue
                
                # Make sure file_data is a list
                if not isinstance(file_data, list):
                    if isinstance(file_data, dict):
                        file_data = [file_data]
                    else:
                        logger.error(f"Invalid data format in {file_key}, deleting...")
                        files_to_delete.append(file_key)
                        continue
                
                # Process each item in the file
                for item in file_data:
                    if not isinstance(item, dict):
                        continue
                        
                    readable_date = item.get('readable_date')
                    if not readable_date:
                        logger.warning(f"Missing readable date in {file_key}, item ignored")
                        continue
                        
                    # Extract date (YYYY-MM-DD)
                    file_date = readable_date.split()[0]
                    
                    if file_date not in data_by_date:
                        data_by_date[file_date] = []
                    
                    data_by_date[file_date].append(item)
                
                # Add file to deletion list and mark as processed
                files_to_delete.append(file_key)
                _compressed_files_cache[vin].add(file_key)
                
            except Exception as e:
                logger.error(f"Error processing file {obj.get('Key')}: {str(e)}")
                files_to_delete.append(obj['Key'])
                _compressed_files_cache[vin].add(obj['Key'])  # Mark as processed to avoid repeated errors
                continue
        
        # If no valid data was found
        if not data_by_date:
            logger.warning(f"No valid data to compress for vehicle {vin}")
            
            # Clean up invalid temporary files
            if files_to_delete:
                delete_tasks = [delete_with_retry(s3_client, bucket_name, key) for key in files_to_delete]
                await asyncio.gather(*delete_tasks)
                
            return True
            
        # Save compressed data by date
        save_tasks = []
        
        for date, items in data_by_date.items():
            if items:
                # If a file already exists for this date, merge the data
                target_key = f"{settings.base_s3_path}/{vin}/{date}.json"
                
                try:
                    # Check if file already exists
                    try:
                        existing_response = await s3_client.get_object(Bucket=bucket_name, Key=target_key)
                        existing_content = await existing_response['Body'].read()
                        existing_data = json.loads(existing_content)
                        
                        # Merge with existing data
                        if isinstance(existing_data, list):
                            items = existing_data + items
                    except ClientError as e:
                        # File doesn't exist, which is normal
                        if e.response['Error']['Code'] != 'NoSuchKey':
                            raise
                
                    # Save merged data
                    save_tasks.append(
                        save_with_retry(s3_client, bucket_name, target_key, items)
                    )
                    
                except Exception as e:
                    logger.error(f"Error merging data for {vin}/{date}: {str(e)}")
        
        # Delete temporary files
        delete_tasks = [delete_with_retry(s3_client, bucket_name, key) for key in files_to_delete]
        
        # Execute all tasks
        await asyncio.gather(*save_tasks, *delete_tasks)
        
        logger.info(f"Compression successful for vehicle {vin}: {len(files_to_delete)} files compressed into {len(data_by_date)} dates")
        return True
        
    except Exception as e:
        logger.error(f"Error compressing data for vehicle {vin}: {str(e)}")
        return False


async def save_with_retry(s3_client, bucket_name: str, key: str, data: Any, max_retries: int = 3) -> bool:
    """
    Saves data to S3 with retry on failure.
    
    Args:
        s3_client: aioboto3 S3 client
        bucket_name: S3 bucket name
        key: S3 key (file path)
        data: Data to save
        max_retries: Maximum number of attempts
        
    Returns:
        bool: True if save was successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            await s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to save to S3 after {max_retries} attempts. Key: {key}, Error: {str(e)}")
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
    
    return False


async def delete_with_retry(s3_client, bucket_name: str, key: str, max_retries: int = 3) -> bool:
    """
    Deletes an S3 file with retry on failure.
    
    Args:
        s3_client: aioboto3 S3 client
        bucket_name: S3 bucket name
        key: S3 key (file path)
        max_retries: Maximum number of attempts
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            await s3_client.delete_object(Bucket=bucket_name, Key=key)
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to delete from S3 after {max_retries} attempts. Key: {key}, Error: {str(e)}")
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
    
    return False


async def cleanup_old_data(retention_days: int = 30) -> bool:
    """
    Cleans up old data beyond the retention period.
    
    Args:
        retention_days: Number of days to retain data
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    logger.info(f"Starting cleanup of data older than {retention_days} days")
    
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    try:
        # List all vehicle prefixes
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{settings.base_s3_path}/",
            Delimiter="/"
        )
        
        if 'CommonPrefixes' not in response:
            logger.info("No vehicles found for cleanup")
            return True
            
        # Create cleanup tasks for each vehicle
        cleanup_tasks = []
        for prefix in response.get('CommonPrefixes', []):
            vehicle_prefix = prefix.get('Prefix', '')
            if vehicle_prefix:
                vin = vehicle_prefix.split('/')[-2]
                task = asyncio.create_task(cleanup_vehicle_data(s3_client, bucket_name, vin, cutoff_date))
                cleanup_tasks.append(task)
        
        if not cleanup_tasks:
            logger.info("No cleanup tasks created")
            return True
            
        # Execute cleanup tasks
        results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        # Check for errors
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        logger.info(f"Cleanup completed: {success_count} vehicles processed successfully, {error_count} errors")
        
        return error_count == 0
        
    except Exception as e:
        logger.error(f"Error during data cleanup: {str(e)}")
        return False


async def cleanup_vehicle_data(s3_client, bucket_name: str, vin: str, cutoff_date: datetime) -> bool:
    """
    Cleans up old data for a specific vehicle.
    
    Args:
        s3_client: aioboto3 S3 client
        bucket_name: S3 bucket name
        vin: Vehicle VIN
        cutoff_date: Cutoff date for retention
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    settings = get_settings()
    vehicle_prefix = f"{settings.base_s3_path}/{vin}/"
    
    try:
        # List all files for the vehicle (excluding temp)
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=vehicle_prefix
        )
        
        if 'Contents' not in response or not response['Contents']:
            return True
            
        files_to_delete = []
        
        for obj in response['Contents']:
            key = obj['Key']
            
            # Ignore temp files
            if "/temp/" in key:
                continue
                
            # Extract date from filename (YYYY-MM-DD.json)
            try:
                filename = key.split('/')[-1]
                if not filename.endswith('.json'):
                    continue
                    
                date_str = filename[:-5]  # Remove .json
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    files_to_delete.append(key)
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Unable to parse date from file {key}: {str(e)}")
                continue
        
        if not files_to_delete:
            logger.debug(f"No files to delete for vehicle {vin}")
            return True
            
        # Delete files in batches to avoid overloading S3
        batch_size = 100
        for i in range(0, len(files_to_delete), batch_size):
            batch = files_to_delete[i:i+batch_size]
            delete_tasks = [delete_with_retry(s3_client, bucket_name, key) for key in batch]
            await asyncio.gather(*delete_tasks)
            
        logger.info(f"Cleanup successful for vehicle {vin}: {len(files_to_delete)} files deleted")
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up data for vehicle {vin}: {str(e)}")
        return False 
