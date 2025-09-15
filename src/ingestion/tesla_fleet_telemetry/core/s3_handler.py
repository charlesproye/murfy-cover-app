import os
import json
import logging
import asyncio
import functools
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
import random
import time
from botocore.client import Config
from botocore.exceptions import ClientError
from functools import wraps

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
            'checksum_validation': False,  # Disable checksum validation properly
            'use_dualstack_endpoint': False
        },
        connect_timeout=5,
        read_timeout=60,
        retries={'max_attempts': 3, 'mode': 'standard'},
        # Add parameter to force clock to use AWS server time
        parameter_validation=True
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
            'checksum_validation': False,  # Disable checksum validation properly
            'use_dualstack_endpoint': False
        },
        connect_timeout=5,
        read_timeout=60,
        retries={'max_attempts': 3, 'mode': 'standard'},
        # Add parameter to force clock to use AWS server time
        parameter_validation=True
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
    # Use retry decorator for put_object operation
    @retry_on_time_skewed(max_retries=3)
    async def put_object_with_retry(client, **kwargs):
        return await client.put_object(**kwargs)
        
    try:
        await put_object_with_retry(
            s3_client,
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



def retry_on_time_skewed(max_retries=3):
    """
    Decorator to retry AWS S3 operations when RequestTimeTooSkewed errors occur.
    Works with both boto3 and aioboto3 clients.
    
    Args:
        max_retries (int): Maximum number of times to retry the operation
    
    Returns:
        Function decorator
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            retry_count = 0
            last_exception = None
            
            while retry_count <= max_retries:
                try:
                    # Apply the helper function to clean kwargs
                    clean_kwargs = _clean_s3_kwargs(kwargs)
                    # Call the original function with cleaned kwargs
                    return await func(*args, **clean_kwargs)
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    
                    if error_code == 'RequestTimeTooSkewed' and retry_count < max_retries:
                        retry_count += 1
                        logger.warning(f"RequestTimeTooSkewed error, retry {retry_count}/{max_retries}")
                        
                        # Get time offset from NTP
                        time_offset = await force_time_sync()
                        
                        if time_offset is None:
                            logger.error("Failed to sync time, retrying without time adjustment")
                        else:
                            # Calculate the correct timestamp for AWS requests
                            logger.info(f"Adjusting time by {time_offset} seconds")
                            
                            # Create a timestamp function that adds the offset
                            def corrected_timestamp():
                                # Get current time and add offset to match NTP time
                                return datetime.utcnow() + timedelta(seconds=time_offset)
                            
                            # Add the corrected timestamp to the kwargs - ensure we don't pass config directly
                            # Instead we store the function for later use, but it will be removed by _clean_s3_kwargs
                            kwargs['timestamp_func'] = corrected_timestamp
                        
                        # Wait a moment before retrying
                        await asyncio.sleep(1)
                    else:
                        # Re-raise if it's not a time skew error or we've exceeded retries
                        raise e
                    
                    last_exception = e
            
            # If we've exhausted retries
            if last_exception:
                raise last_exception
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            retry_count = 0
            last_exception = None
            
            while retry_count <= max_retries:
                try:
                    # Apply the helper function to clean kwargs
                    clean_kwargs = _clean_s3_kwargs(kwargs)
                    # Call the original function with cleaned kwargs
                    return func(*args, **clean_kwargs)
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    
                    if error_code == 'RequestTimeTooSkewed' and retry_count < max_retries:
                        retry_count += 1
                        logger.warning(f"RequestTimeTooSkewed error, retry {retry_count}/{max_retries}")
                        
                        # For synchronous calls, we need to run force_time_sync in a separate thread
                        loop = asyncio.new_event_loop()
                        time_offset = loop.run_until_complete(force_time_sync())
                        loop.close()
                        
                        if time_offset is None:
                            logger.error("Failed to sync time, retrying without time adjustment")
                        else:
                            # Calculate the correct timestamp for AWS requests
                            logger.info(f"Adjusting time by {time_offset} seconds")
                            
                            # Create a timestamp function that adds the offset
                            def corrected_timestamp():
                                # Get current time and add offset to match NTP time
                                return datetime.utcnow() + timedelta(seconds=time_offset)
                            
                            # Add the corrected timestamp to the kwargs but don't pass config directly
                            # It will be cleaned by _clean_s3_kwargs
                            kwargs['timestamp_func'] = corrected_timestamp
                        
                        # Wait a moment before retrying
                        time.sleep(1)
                    else:
                        # Re-raise if it's not a time skew error or we've exceeded retries
                        raise e
                    
                    last_exception = e
            
            # If we've exhausted retries
            if last_exception:
                raise last_exception
        
        # Detect if the wrapped function is a coroutine function
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator



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
    # Define put_object with time skew retry
    @retry_on_time_skewed(max_retries=3)
    async def put_object_with_retry(client, **kwargs):
        return await client.put_object(**kwargs)
    
    for attempt in range(max_retries):
        try:
            await put_object_with_retry(
                s3_client,
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            # If this is a RequestTimeTooSkewed error, let the decorator handle it
            if e.response.get('Error', {}).get('Code') == 'RequestTimeTooSkewed':
                raise
            # For other ClientErrors, handle as before
            if attempt == max_retries - 1:
                logger.error(f"Failed to save to S3 after {max_retries} attempts. Key: {key}, Error: {str(e)}")
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
        except Exception as e:
            # Handle other exceptions
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
    # Define delete_object with time skew retry
    @retry_on_time_skewed(max_retries=3)
    async def delete_object_with_retry(client, **kwargs):
        return await client.delete_object(**kwargs)
    
    for attempt in range(max_retries):
        try:
            await delete_object_with_retry(
                s3_client,
                Bucket=bucket_name,
                Key=key
            )
            return True
        except ClientError as e:
            # If this is a RequestTimeTooSkewed error, let the decorator handle it
            if e.response.get('Error', {}).get('Code') == 'RequestTimeTooSkewed':
                raise
            # For other ClientErrors, handle as before
            if attempt == max_retries - 1:
                logger.error(f"Failed to delete from S3 after {max_retries} attempts. Key: {key}, Error: {str(e)}")
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
        except Exception as e:
            # Handle other exceptions
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
        # Define list_objects_v2 with retry
        @retry_on_time_skewed(max_retries=3)
        async def list_objects_v2_with_retry(client, **kwargs):
            return await client.list_objects_v2(**kwargs)
            
        # List all vehicle prefixes with retry
        response = await list_objects_v2_with_retry(
            s3_client,
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
        # Define list_objects_v2 with retry
        @retry_on_time_skewed(max_retries=3)
        async def list_objects_v2_with_retry(client, **kwargs):
            return await client.list_objects_v2(**kwargs)
            
        # List all files for the vehicle (excluding temp) with retry
        response = await list_objects_v2_with_retry(
            s3_client,
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



async def sync_time_with_aws() -> Optional[float]:
    """
    Attempts to synchronize time with AWS S3 server by making a request and 
    checking for time skew errors. Returns the detected time offset if found.
    
    Returns:
        Optional[float]: Time offset in seconds, or None if couldn't determine
    """
    logger.info("Checking time synchronization with AWS S3...")
    
    # First try using NTP (fastest and most reliable)
    try:
        ntp_offset = await force_time_sync()
        if ntp_offset is not None:
            logger.info(f"Time synchronized with NTP: offset {ntp_offset:.2f} seconds")
            return ntp_offset
    except Exception as e:
        logger.warning(f"Could not sync with NTP: {str(e)}")
    
    # Fallback to AWS S3 check
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    
    try:
        # Make a simple LIST request that should return quickly
        try:
            # Define a simple retry function for this operation
            @retry_on_time_skewed(max_retries=3)
            async def list_objects_v2_simple(client, **kwargs):
                return await client.list_objects_v2(**kwargs)
                
            # Try a simple list operation first to check connectivity
            await list_objects_v2_simple(
                s3_client,
                Bucket=bucket_name,
                MaxKeys=1
            )
            logger.info("Time appears to be in sync with AWS S3")
            return None
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            
            if error_code == 'RequestTimeTooSkewed':
                # Our retry mechanism should have detected the time offset
                # Extract it from the decorator's global state
                for obj in globals().values():
                    if callable(obj) and obj.__name__ == 'retry_on_time_skewed':
                        # Find instances of our decorator
                        for attr_name in dir(obj):
                            if attr_name.startswith('global_time_offset'):
                                time_offset = getattr(obj, attr_name)[0]
                                if time_offset != 0:
                                    logger.warning(f"Detected time offset with AWS: {time_offset:.2f} seconds")
                                    return time_offset
                
                # Fall back to error message parsing as before
                error_message = e.response.get('Error', {}).get('Message', '')
                logger.warning(f"Time skew detected: {error_message}")
                
                import re
                match = re.search(r'server time is approx ([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)', error_message)
                if match:
                    aws_time_str = match.group(1)
                    try:
                        aws_time = datetime.strptime(aws_time_str, '%Y-%m-%dT%H:%M:%SZ')
                        local_time = datetime.utcnow()
                        time_offset = (aws_time - local_time).total_seconds()
                        
                        logger.warning(f"System time is {abs(time_offset):.2f} seconds {'behind' if time_offset > 0 else 'ahead of'} AWS time")
                        logger.warning(f"AWS time: {aws_time.isoformat()}")
                        logger.warning(f"Local time: {local_time.isoformat()}")
                        
                        # Recommend NTP synchronization if skew is significant
                        if abs(time_offset) > 60:  # More than a minute
                            logger.warning("IMPORTANT: System clock is significantly skewed. Consider synchronizing "
                                           "your system time using NTP: 'sudo ntpdate pool.ntp.org'")
                        
                        return time_offset
                    except Exception as parse_err:
                        logger.error(f"Error parsing AWS time: {str(parse_err)}")
                
                # If we still don't have an offset, use NTP as last resort
                return await force_time_sync()
            else:
                # Different error
                logger.error(f"Unexpected error checking time sync: {str(e)}")
                return None
    except Exception as e:
        logger.error(f"Error checking time synchronization: {str(e)}")
        return None


# Add the function to __all__ export
__all__ = [
    "save_data_to_s3",
    "cleanup_old_data",
    "get_s3_async_client",
    "get_s3_sync_client",
    "sync_time_with_aws",
    "force_time_sync"
]

# Helper function to manually sync time
async def force_time_sync():
    """
    Force time synchronization with network time protocol.
    
    Returns:
        float or None: Time offset in seconds between local and NTP time
                      or None if synchronization failed
    """
    try:
        import socket, struct, time
        
        # NTP query packet (mode=3, version=3)
        NTP_PACKET = b'\x1b' + 47 * b'\0'
        
        # Try multiple NTP servers in case one fails
        NTP_SERVERS = ["pool.ntp.org", "time.google.com", "time.apple.com", "time.windows.com"]
        
        # Track any errors for logging
        errors = []
        
        for server in NTP_SERVERS:
            try:
                # Create UDP socket
                client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client.settimeout(3)  # 3-second timeout
                
                # Send request
                try:
                    client.sendto(NTP_PACKET, (server, 123))
                    msg, _ = client.recvfrom(1024)
                    
                    # Extract timestamp
                    t = struct.unpack("!12I", msg)[10] - 2208988800  # Convert to UNIX epoch
                    
                    # Get NTP and local time
                    ntp_time = datetime.fromtimestamp(t)
                    local_time = datetime.now()
                    
                    # Calculate offset (NTP - local)
                    time_offset = (ntp_time - local_time).total_seconds()
                    
                    logger.info(f"Successfully synced with NTP server {server}")
                    
                    # Don't accept extremely large offsets as they're likely errors
                    if abs(time_offset) > 31536000:  # 1 year in seconds
                        logger.warning(f"Unrealistic time offset from {server}: {time_offset} seconds, ignoring")
                        continue
                        
                    return time_offset
                    
                except Exception as e:
                    errors.append(f"Error with {server}: {str(e)}")
                    continue
                finally:
                    client.close()
                    
            except Exception as e:
                errors.append(f"Socket error with {server}: {str(e)}")
                continue
        
        # If we get here, all servers failed
        if errors:
            logger.warning(f"All NTP servers failed: {'; '.join(errors)}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error in NTP synchronization: {str(e)}")
        return None



def _clean_s3_kwargs(kwargs):
    """
    Clean S3 kwargs by removing problematic parameters.
    
    Args:
        kwargs (dict): The keyword arguments to clean
        
    Returns:
        dict: The cleaned kwargs
    """
    # Make a copy to avoid modifying the original
    clean_kwargs = kwargs.copy()
    
    # Remove config parameter which causes issues
    if 'config' in clean_kwargs:
        del clean_kwargs['config']
        
    # Remove timestamp_func which is used internally by retry_on_time_skewed
    if 'timestamp_func' in clean_kwargs:
        del clean_kwargs['timestamp_func']
        
    return clean_kwargs
