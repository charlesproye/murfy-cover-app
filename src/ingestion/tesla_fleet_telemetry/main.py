import argparse
import asyncio
import logging
import os
import json
import signal
import sys
import time
import warnings
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import aiohttp
import aioboto3
from botocore.client import Config
        

# Ignore aiohttp ResourceWarnings due to unclosed sockets
warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<socket.socket.*>")

# Fix relative imports to use absolute paths
from ingestion.tesla_fleet_telemetry.utils.kafka_consumer import KafkaConsumer
from ingestion.tesla_fleet_telemetry.utils.data_processor import process_telemetry_data
from ingestion.tesla_fleet_telemetry.core.s3_handler import compress_data, save_data_to_s3, cleanup_old_data, cleanup_clients
from ingestion.tesla_fleet_telemetry.config.settings import get_settings


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("tesla-fleet-telemetry")

# Shutdown management events
shutdown_event = asyncio.Event()
compression_event = asyncio.Event()
should_exit = asyncio.Event()

# Data buffers by VIN
data_buffers: Dict[str, List[Dict[str, Any]]] = {}
last_flush_time: Dict[str, float] = {}
messages_processed = 0
running = True

# Maximum buffer size before flush (number of messages)
MAX_BUFFER_SIZE = 3000
# Flush interval in seconds
FLUSH_INTERVAL_SECONDS = 30
# Max number of vehicles processed per worker in the pool
MAX_VEHICLES_PER_WORKER = 200
# Maximum number of workers in the pool
MAX_WORKERS = 50
# Default compression interval (in seconds)
DEFAULT_COMPRESSION_INTERVAL = 300
# Cleanup interval (in seconds)
CLEANUP_INTERVAL = 86400

async def setup_logging(verbose: bool = False):
    """Configure the logging system."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Formatter for logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(console_handler)
    
    # Specific loggers
    for logger_name in ["tesla-fleet-telemetry", "kafka-consumer", "s3-handler"]:
        specific_logger = logging.getLogger(logger_name)
        specific_logger.setLevel(log_level)
        
    logger.info(f"Logging configured with level {logging.getLevelName(log_level)}")

def signal_handler(sig, frame):
    """Signal handler for graceful process termination."""
    global running
    logger.info(f"Signal {sig} received, shutting down...")
    running = False
    
    # Trigger shutdown event in a thread-safe manner
    asyncio.get_event_loop().call_soon_threadsafe(shutdown_event.set)
    
    # Set a timer to force exit after 5 seconds
    def force_exit():
        logger.warning("Shutdown timeout exceeded, forcing exit")
        sys.exit(1)
    
    # Schedule a forced shutdown after 5 seconds
    signal.signal(signal.SIGALRM, lambda sig, frame: force_exit())
    signal.alarm(5)

async def graceful_shutdown():
    """Gracefully shut down all running tasks."""
    logger.info("Graceful shutdown in progress...")
    
    try:
        # Final buffer flush
        logger.info("Final buffer flush...")
        
        # Add timeout to flush to prevent indefinite blocking
        try:
            await asyncio.wait_for(flush_all_buffers(), timeout=3.0)
            logger.info("Flush completed successfully")
        except asyncio.TimeoutError:
            logger.warning("Timeout during buffer flush")
        
        # Cleanup S3 client connections
        logger.info("Cleaning up S3 connections...")
        try:
            await asyncio.wait_for(cleanup_clients(), timeout=2.0)
            logger.info("S3 connections closed successfully")
        except asyncio.TimeoutError:
            logger.warning("Timeout during S3 client cleanup")
        except Exception as e:
            logger.error(f"Error during S3 client cleanup: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error during graceful shutdown: {str(e)}")
    
    logger.info("Graceful shutdown completed")

async def process_message(message) -> Optional[Dict[str, Any]]:
    """
    Process a Kafka message by extracting relevant information.
    
    Args:
        message: Kafka message (ConsumerRecord or dict)
        
    Returns:
        Dict containing structured data or None if the message is invalid
    """
    try:
        # Extract message value if it's a ConsumerRecord
        if hasattr(message, 'value'):
            try:
                # Try to decode JSON message
                data = json.loads(message.value.decode('utf-8'))
                logger.debug(f"Decoded message: {data}")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Unable to decode message: {str(e)}")
                return None
        else:
            # If it's already a dict, use it directly
            data = message
            
        if not isinstance(data, dict):
            logger.warning(f"Invalid message format: {type(data)}")
            return None
            
        # Extract relevant fields
        vin = data.get("vin")
        if not vin:
            logger.warning("Message without VIN ignored")
            return None
            
        # Normalize VIN (uppercase, remove spaces)
        vin = vin.strip().upper()
        
        # Extract timestamp or createdAt and convert to readable date
        timestamp = data.get("timestamp")
        created_at = data.get("createdAt")
        
        if not timestamp and not created_at:
            logger.warning(f"Message without timestamp or createdAt ignored for VIN {vin}")
            return None
            
        try:
            # If we have an ISO createdAt, convert it to timestamp
            if created_at and not timestamp:
                try:
                    # Convert ISO format "2025-03-19T15:43:06.516737697Z" to timestamp
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000)  # milliseconds
                    logger.debug(f"createdAt {created_at} converted to timestamp {timestamp}")
                except Exception as e:
                    logger.warning(f"Unable to convert createdAt {created_at} to timestamp: {str(e)}")
                    timestamp = int(datetime.now().timestamp() * 1000)
            
            # Convert to milliseconds if necessary (some timestamps are in seconds)
            if timestamp and len(str(timestamp)) <= 10:
                timestamp = int(timestamp) * 1000
                
            # Convert to datetime and readable string
            dt = datetime.fromtimestamp(timestamp / 1000)
            readable_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting timestamp {timestamp} for VIN {vin}: {str(e)}")
            readable_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            timestamp = int(datetime.now().timestamp() * 1000)
        
        # Build structured data object
        structured_data = {
            "vin": vin,
            "timestamp": timestamp,
            "readable_date": readable_date,
            "data": data.get("data", {}),
            "meta": data.get("meta", {})
        }
        
        # Add original createdAt if it exists
        if created_at:
            structured_data["createdAt"] = created_at
            
        logger.debug(f"Structured message: {structured_data}")
        return structured_data
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}", exc_info=True)
        return None

async def add_to_buffer(data: Dict[str, Any]):
    """
    Add data to buffer with backpressure management.
    
    Args:
        data: Structured data to add to buffer
    """
    global data_buffers, last_flush_time, messages_processed
    
    vin = data.get("vin")
    if not vin:
        return
        
    # Initialize buffer and last flush timestamp if needed
    if vin not in data_buffers:
        data_buffers[vin] = []
        last_flush_time[vin] = asyncio.get_event_loop().time()
    
    # Check total buffer size (backpressure)
    total_messages = sum(len(buffer) for buffer in data_buffers.values())
    if total_messages >= MAX_BUFFER_SIZE * len(data_buffers):
        logger.warning(f"Backpressure: {total_messages} messages in buffer, forcing flush")
        await flush_all_buffers()
    
    # Add data to buffer
    data_buffers[vin].append(data)
    messages_processed += 1
    
    # Periodic log of processed messages
    if messages_processed % 1000 == 0:
        logger.info(f"{messages_processed} messages processed, {len(data_buffers)} vehicles in buffer")
    
    # Auto flush if buffer reaches maximum size
    if len(data_buffers[vin]) >= MAX_BUFFER_SIZE:
        await flush_buffer(vin)

async def flush_buffer(vin: str) -> bool:
    """
    Send buffer data to S3 and empty the buffer.
    
    Args:
        vin: VIN of the vehicle whose buffer should be flushed
        
    Returns:
        bool: True if flush was successful, False otherwise
    """
    global data_buffers, last_flush_time
    
    if vin not in data_buffers or not data_buffers[vin]:
        return True
        
    buffer_data = data_buffers[vin]
    buffer_size = len(buffer_data)
    
    logger.debug(f"Flushing buffer for VIN {vin} ({buffer_size} messages)")
    
    try:
        # Send data to S3
        success = await save_data_to_s3(buffer_data, vin)
        
        if success:
            # Empty buffer and update timestamp
            data_buffers[vin] = []
            last_flush_time[vin] = asyncio.get_event_loop().time()
            logger.debug(f"Flush successful for VIN {vin} ({buffer_size} messages)")
            return True
        else:
            logger.error(f"Flush failed for VIN {vin}")
            return False
            
    except Exception as e:
        logger.error(f"Error during buffer flush for VIN {vin}: {str(e)}")
        return False

async def check_buffer_timeouts():
    """
    Check buffer timeouts and trigger flush if necessary.
    """
    current_time = asyncio.get_event_loop().time()
    tasks = []
    
    # For each VIN with a buffer
    for vin, last_time in last_flush_time.items():
        # If buffer exceeded flush interval
        if (current_time - last_time) >= FLUSH_INTERVAL_SECONDS and vin in data_buffers and data_buffers[vin]:
            tasks.append(flush_buffer(vin))
    
    # Execute flush tasks in parallel
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if r is False or isinstance(r, Exception))
        
        if error_count > 0:
            logger.warning(f"Periodic flush: {success_count} successful, {error_count} failed")

async def flush_all_buffers():
    """
    Flush all pending buffers.
    """
    tasks = []
    
    for vin in list(data_buffers.keys()):
        if data_buffers[vin]:
            tasks.append(flush_buffer(vin))
    
    if tasks:
        logger.info(f"Flushing all buffers ({len(tasks)} vehicles)")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if r is False or isinstance(r, Exception))
        
        logger.info(f"Global flush: {success_count} successful, {error_count} failed")

async def create_vehicle_worker_pools(vehicles: List[str]):
    """
    Distribute vehicles into balanced pools for parallel processing.
    
    Args:
        vehicles: List of vehicle VINs to process
        
    Returns:
        List[List[str]]: List of vehicle pools
    """
    # Limit the number of vehicles per worker
    vehicles_per_worker = min(MAX_VEHICLES_PER_WORKER, 
                             (len(vehicles) + MAX_WORKERS - 1) // MAX_WORKERS)
    
    # Distribute vehicles into pools
    pools = []
    for i in range(0, len(vehicles), vehicles_per_worker):
        pools.append(vehicles[i:i + vehicles_per_worker])
    
    logger.info(f"Created {len(pools)} vehicle pools ({len(vehicles)} vehicles total)")
    return pools

async def compress_worker(vehicles: List[str], date: datetime):
    """
    Compression worker for a set of vehicles for a given date.
    
    Args:
        vehicles: List of VINs to compress
        date: Date of data to compress (UTC)
    """
    settings = get_settings()
    s3_client = None
    
    try:        
        session = aioboto3.Session()
        
        # Use the same config as in s3_handler
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
        
        s3_client = await session.client(
            's3',
            region_name=settings.s3_region,
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_key,
            aws_secret_access_key=settings.s3_secret,
            config=boto_config,
            verify=False  # Disable SSL verification if using a self-signed cert
        ).__aenter__()
        
        # Date format for S3 paths
        date_path = date.strftime("%Y/%m/%d")
        
        for vin in vehicles:
            if not running:
                break
                
            try:
                # Compressed file path
                compressed_path = f"{settings.base_s3_path}/compressed/{date_path}/{vin}.parquet"
                
                # Compress vehicle data for the specified date
                await compress_vehicle_data_for_date(s3_client, settings.s3_bucket, vin, date, compressed_path)
                
            except Exception as e:
                logger.error(f"Error compressing vehicle {vin} for date {date}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in compression worker: {str(e)}")
    finally:
        # Ensure client is properly closed
        if s3_client:
            try:
                await s3_client.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error closing S3 client: {str(e)}")

async def is_midnight() -> bool:
    """Check if it's midnight UTC."""
    now = datetime.now()
    print("test", now.hour, now.minute)
    return now.hour ==0 and now.minute == 0

async def compress_previous_day_data():
    """
    Compress previous day's data and organize by date.
    """
    logger.info("Starting compression of previous day's data")
    
    try:
        # Calculate yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get list of vehicles with temporary data
        settings = get_settings()

        print("yesterday", yesterday)
        session = aioboto3.Session()
        
        # Use the same config as in s3_handler
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
        
        s3_client = await session.client(
            's3',
            region_name=settings.s3_region,
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_key,
            aws_secret_access_key=settings.s3_secret,
            config=boto_config,
            verify=False  # Disable SSL verification if using a self-signed cert
        ).__aenter__()
        
        vehicles = []
        
        try:
            # Get list of vehicles
            response = await s3_client.list_objects_v2(
                Bucket=settings.s3_bucket,
                Prefix=f"{settings.base_s3_path}/temp/",
                Delimiter="/"
            )
            
            for prefix in response.get('CommonPrefixes', []):
                vehicle_prefix = prefix.get('Prefix', '')
                if vehicle_prefix:
                    vin = vehicle_prefix.split('/')[-2]
                    vehicles.append(vin)
        finally:
            # Ensure client is properly closed
            try:
                await s3_client.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error closing S3 client: {str(e)}")
        
        if not vehicles:
            logger.info("No vehicles found to compress")
            return
            
        # Create vehicle pools
        vehicle_pools = await create_vehicle_worker_pools(vehicles)
        
        # Launch compression workers with yesterday's date
        worker_tasks = [asyncio.create_task(compress_worker(pool, yesterday)) 
                       for pool in vehicle_pools]
        
        # Wait for all workers to complete
        await asyncio.gather(*worker_tasks)
        
        logger.info(f"Compression of data for {yesterday.date()} completed")
        
        # Clean up yesterday's temporary files
        await cleanup_old_data(yesterday)
        
    except Exception as e:
        logger.error(f"Error during daily compression: {str(e)}")

async def consume_kafka_data(topic: str, group_id: str, bootstrap_servers: str, 
                            auto_offset_reset: str = "latest", buffer_flush_interval: int = 30,
                            buffer_size: int = 3000):
    """
    Consume Kafka data with midnight daily compression.
    """
    global MAX_BUFFER_SIZE, FLUSH_INTERVAL_SECONDS, running
    
    MAX_BUFFER_SIZE = buffer_size
    FLUSH_INTERVAL_SECONDS = buffer_flush_interval
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer = KafkaConsumer(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_interval_ms=300000,
        session_timeout_ms=60000,
        request_timeout_ms=30000
    )
    
    logger.info(f"Starting Kafka consumer: {topic}, {group_id}, {bootstrap_servers}")
    logger.info(f"Configuration: buffer={MAX_BUFFER_SIZE}, flush_interval={FLUSH_INTERVAL_SECONDS}s")
    
    last_midnight_check = datetime.now() - timedelta(days=1)
    timeout_task = None
    
    try:
        async def check_timeouts():
            nonlocal last_midnight_check
            while not shutdown_event.is_set():
                try:
                    await check_buffer_timeouts()
                    print("check_timeouts")
                    # Check if it's midnight
                    now = datetime.now()
                    if now.date() > last_midnight_check.date():
                        if await is_midnight():
                            print("is_midnight ok")
                            logger.info("Midnight UTC detected, starting daily compression")
                            await compress_data() ######ompress_previous_day_data()
                            last_midnight_check = now
                            
                except Exception as e:
                    logger.error(f"Error checking timeouts: {str(e)}")
                
                try:
                    # Use wait_for with timeout to periodically check shutdown_event
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0) # check fait toutes les 30 secondes
                    break
                except asyncio.TimeoutError:
                    continue
        
        timeout_task = asyncio.create_task(check_timeouts())
        
        while not shutdown_event.is_set():
            try:
                async for message in consumer.consume():
                    if shutdown_event.is_set():
                        break
                        
                    try:
                        processed_message = await process_message(message)
                        if processed_message:
                            await add_to_buffer(processed_message)
                            
                    except Exception as e:
                        logger.error(f"Error processing messages: {str(e)}")
                        if not shutdown_event.is_set():
                            await asyncio.sleep(1)
                            
            except Exception as e:
                if not shutdown_event.is_set():
                    logger.error(f"Error in Kafka consumption loop: {str(e)}")
                    await asyncio.sleep(1)
                else:
                    break
    
    except Exception as e:
        logger.error(f"Error in main loop: {str(e)}")
    finally:
        logger.info("Final cleanup...")
        
        # Cancel timeout checking task
        if timeout_task:
            timeout_task.cancel()
            try:
                await timeout_task
            except asyncio.CancelledError:
                pass
        
        # Graceful shutdown
        await graceful_shutdown()
        
        # Close Kafka consumer
        await consumer.close()
        
        logger.info("Cleanup completed")

async def run_compress_now():
    """
    Execute immediate compression of all data.
    """
    logger.info("Starting immediate data compression")
    
    try:
        # Run regular compression, not the non-existent compress_data_parallel
        await compress_data()
        logger.info("Immediate compression completed")
        return True
    except Exception as e:
        logger.error(f"Error during immediate compression: {str(e)}")
        return False

async def main():
    """
    Main application function.
    """
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(description='Tesla Fleet Telemetry - Data Ingestion')
        parser.add_argument('--compress-now', action='store_true', 
                           help='Compress data immediately and exit')
        parser.add_argument('--verbose', action='store_true', 
                           help='Enable verbose logging (debug)')
        parser.add_argument('--topic', type=str, 
                           help='Kafka topic to consume')
        parser.add_argument('--group-id', type=str, 
                           help='Kafka consumer group ID')
        parser.add_argument('--bootstrap-servers', type=str, 
                           help='Kafka bootstrap servers (comma-separated)')
        parser.add_argument('--auto-offset-reset', type=str, choices=['earliest', 'latest'], 
                           default='latest', 
                           help='Starting position (earliest or latest)')
        parser.add_argument('--buffer-size', type=int, default=3000, 
                           help='Maximum buffer size before automatic flush')
        parser.add_argument('--buffer-flush-interval', type=int, default=30, 
                           help='Buffer flush interval in seconds')
        parser.add_argument('--disable-periodic-compression', action='store_true', 
                           help='Disable periodic compression')
        parser.add_argument('--compression-interval', type=int, default=300, 
                           help='Compression interval in seconds')
        parser.add_argument(
        "--compress_time",
        type=str,
        default="00:00",
        help="time of day at which to compress S3 data",
        )

        
        args = parser.parse_args()
        
        # Logging configuration
        await setup_logging(verbose=args.verbose)
        
        # Default configuration
        settings = get_settings()
        
        # Use command-line arguments or default values
        topic = args.topic or settings.kafka_topic
        group_id = args.group_id or settings.kafka_group_id
        bootstrap_servers = args.bootstrap_servers or settings.kafka_bootstrap_servers
        auto_offset_reset = args.auto_offset_reset
        buffer_size = args.buffer_size
        buffer_flush_interval = args.buffer_flush_interval
        compress_time = args.compress_time
        # Immediate compression if requested
        if args.compress_now or os.getenv("COMPRESS_ONLY_TESLA") == "1":
            logger.info("Immediate compression mode")
            await run_compress_now()
            return
        
        # Start Kafka consumption
        await consume_kafka_data(
            topic=topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            buffer_size=buffer_size,
            buffer_flush_interval=buffer_flush_interval,
        )
    
    except KeyboardInterrupt:
        logger.info("Keyboard interruption detected")
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}", exc_info=True)
    finally:
        # Ensure shutdown event is set
        shutdown_event.set()
        logger.info("Program terminated")
        
        # Force exit after 1 second
        logger.debug("Process ending in 1 second...")
        time.sleep(1)
        sys.exit(0)

if __name__ == "__main__":
    try:
        # Configure aiohttp to avoid the unclosed client session error
        if hasattr(aiohttp, 'ClientSession'):
            original_init = aiohttp.ClientSession.__init__
            
            def patched_init(self, *args, **kwargs):
                kwargs['connector_owner'] = False  # Avoid connector warning
                return original_init(self, *args, **kwargs)
                
            aiohttp.ClientSession.__init__ = patched_init
            
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interruption during startup")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1) 
