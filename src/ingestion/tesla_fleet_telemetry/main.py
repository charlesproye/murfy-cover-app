import asyncio
import contextlib
import json
import logging
import signal
import sys
import time
import warnings
from datetime import datetime
from typing import Any

import click

# Fix relative imports to use absolute paths
from ingestion.tesla_fleet_telemetry.config.settings import get_settings
from ingestion.tesla_fleet_telemetry.services.data_storage import (
    cleanup_clients,
    save_data_to_s3,
)
from ingestion.tesla_fleet_telemetry.utils.kafka_consumer import KafkaConsumer

warnings.filterwarnings(
    "ignore", category=ResourceWarning, message="unclosed.*<socket.socket.*>"
)
# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("tesla-fleet-telemetry")

# Shutdown management events
shutdown_event = asyncio.Event()


# Data buffers by VIN
data_buffers: dict[str, list[dict[str, Any]]] = {}
last_flush_time: dict[str, float] = {}
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
# VIN cleanup interval (in seconds) - remove inactive VINs to prevent memory leak
VIN_CLEANUP_INTERVAL = 3600  # Check every hour
VIN_INACTIVE_THRESHOLD = 86400  # Remove VINs inactive for 24 hours


async def setup_logging(verbose: bool = False):
    """Configure the logging system."""
    log_level = logging.DEBUG if verbose else logging.INFO

    # Formatter for logs
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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
        except TimeoutError:
            logger.warning("Timeout during buffer flush")

        # Cleanup S3 client connections
        logger.info("Cleaning up S3 connections...")
        try:
            await asyncio.wait_for(cleanup_clients(), timeout=2.0)
            logger.info("S3 connections closed successfully")
        except TimeoutError:
            logger.warning("Timeout during S3 client cleanup")
        except Exception as e:
            logger.error(f"Error during S3 client cleanup: {e!s}")

    except Exception as e:
        logger.error(f"Error during graceful shutdown: {e!s}")

    logger.info("Graceful shutdown completed")


async def process_message(message) -> dict[str, Any] | None:
    """
    Process a Kafka message by extracting relevant information.

    Args:
        message: Kafka message (ConsumerRecord or dict)

    Returns:
        Dict containing structured data or None if the message is invalid
    """
    try:
        # Extract message value if it's a ConsumerRecord
        if hasattr(message, "value"):
            try:
                # Try to decode JSON message
                data = json.loads(message.value.decode("utf-8"))
                logger.debug(f"Decoded message: {data}")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(
                    f"Unable to decode message: {e!s}. Message: {message.value}"
                )
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
            logger.warning(
                f"Message without timestamp or createdAt ignored for VIN {vin}"
            )
            return None

        try:
            # If we have an ISO createdAt, convert it to timestamp
            if created_at and not timestamp:
                try:
                    # Convert ISO format "2025-03-19T15:43:06.516737697Z" to timestamp
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    timestamp = int(dt.timestamp() * 1000)  # milliseconds
                    logger.debug(
                        f"createdAt {created_at} converted to timestamp {timestamp}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to convert createdAt {created_at} to timestamp: {e!s}"
                    )
                    timestamp = int(datetime.now().timestamp() * 1000)

            # Convert to milliseconds if necessary (some timestamps are in seconds)
            if timestamp and len(str(timestamp)) <= 10:
                timestamp = int(timestamp) * 1000

            # Convert to datetime and readable string
            dt = datetime.fromtimestamp(timestamp / 1000)
            readable_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Error converting timestamp {timestamp} for VIN {vin}: {e!s}"
            )
            readable_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            timestamp = int(datetime.now().timestamp() * 1000)

        # Build structured data object
        structured_data = {
            "vin": vin,
            "timestamp": timestamp,
            "readable_date": readable_date,
            "data": data.get("data", {}),
            "meta": data.get("meta", {}),
        }

        # Add original createdAt if it exists
        if created_at:
            structured_data["createdAt"] = created_at

        logger.debug(f"Structured message: {structured_data}")
        return structured_data

    except Exception as e:
        logger.error(f"Error processing message: {e!s}", exc_info=True)
        return None


async def add_to_buffer(data: dict[str, Any]):
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
        logger.warning(
            f"Backpressure: {total_messages} messages in buffer, forcing flush"
        )
        await flush_all_buffers()

    # Add data to buffer
    data_buffers[vin].append(data)
    messages_processed += 1

    # Periodic log of processed messages
    if messages_processed % 1000 == 0:
        logger.info(
            f"{messages_processed} messages processed, {len(data_buffers)} vehicles in buffer"
        )

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

    # Swap buffer to avoid race condition with add_to_buffer
    buffer_data = data_buffers[vin]
    data_buffers[vin] = []
    buffer_size = len(buffer_data)

    logger.debug(f"Flushing buffer for VIN {vin} ({buffer_size} messages)")

    try:
        # Send data to S3
        success = await save_data_to_s3(buffer_data, vin)

        if success:
            last_flush_time[vin] = asyncio.get_event_loop().time()
            logger.debug(f"Flush successful for VIN {vin} ({buffer_size} messages)")
            return True
        else:
            logger.error(f"Flush failed for VIN {vin}")
            # Restore data on failure (putting it back at the start)
            data_buffers[vin] = buffer_data + data_buffers[vin]
            return False

    except Exception as e:
        logger.error(f"Error during buffer flush for VIN {vin}: {e!s}")
        # Restore data on exception
        data_buffers[vin] = buffer_data + data_buffers[vin]
        return False


async def check_buffer_timeouts(flush_interval: int = FLUSH_INTERVAL_SECONDS):
    """
    Check buffer timeouts and trigger flush if necessary.

    Args:
        flush_interval: Interval in seconds to trigger a flush
    """
    current_time = asyncio.get_event_loop().time()
    tasks = []

    # For each VIN with a buffer
    for vin, last_time in last_flush_time.items():
        # If buffer exceeded flush interval
        if (
            (current_time - last_time) >= flush_interval
            and vin in data_buffers
            and data_buffers[vin]
        ):
            tasks.append(flush_buffer(vin))

    # Execute flush tasks in parallel
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if r is False or isinstance(r, Exception))

        if error_count > 0:
            logger.warning(
                f"Periodic flush: {success_count} successful, {error_count} failed"
            )
        else:
            logger.info(f"Periodic flush: {success_count} successful")


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


async def cleanup_inactive_vins():
    """
    Periodically remove inactive VINs from tracking dictionaries to prevent memory leak.
    This task runs in the background and cleans up VINs that haven't received data
    for more than VIN_INACTIVE_THRESHOLD seconds.
    """
    global data_buffers, last_flush_time, running

    while running:
        await asyncio.sleep(VIN_CLEANUP_INTERVAL)

        if not running:
            break

        current_time = asyncio.get_event_loop().time()
        inactive_vins = []

        for vin, last_time in last_flush_time.items():
            # If VIN hasn't been seen in VIN_INACTIVE_THRESHOLD seconds and buffer is empty
            if (
                (current_time - last_time) > VIN_INACTIVE_THRESHOLD
                and vin in data_buffers
                and not data_buffers[vin]
            ):
                inactive_vins.append(vin)

        for vin in inactive_vins:
            data_buffers.pop(vin, None)
            last_flush_time.pop(vin, None)

        if inactive_vins:
            logger.info(
                f"Cleaned up {len(inactive_vins)} inactive VINs. "
                f"Active VINs remaining: {len(data_buffers)}"
            )
            logger.debug(
                f"Removed VINs: {inactive_vins[:10]}{'...' if len(inactive_vins) > 10 else ''}"
            )


async def create_vehicle_worker_pools(vehicles: list[str]):
    """
    Distribute vehicles into balanced pools for parallel processing.

    Args:
        vehicles: List of vehicle VINs to process

    Returns:
        List[List[str]]: List of vehicle pools
    """
    # Limit the number of vehicles per worker
    vehicles_per_worker = min(
        MAX_VEHICLES_PER_WORKER, (len(vehicles) + MAX_WORKERS - 1) // MAX_WORKERS
    )

    # Distribute vehicles into pools
    pools = []
    for i in range(0, len(vehicles), vehicles_per_worker):
        pools.append(vehicles[i : i + vehicles_per_worker])

    logger.info(f"Created {len(pools)} vehicle pools ({len(vehicles)} vehicles total)")
    return pools


async def consume_kafka_data(
    topic: str,
    group_id: str,
    bootstrap_servers: str,
    auto_offset_reset: str = "latest",
    buffer_flush_interval: int = 30,
):
    """Consume data from Kafka topic and process it."""
    global running, messages_processed

    logger.info(f"Starting Kafka consumer for topic {topic}")
    consumer = KafkaConsumer(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset_reset,
    )

    # Start background tasks for periodic operations
    async def periodic_flush_check():
        """Periodically check for buffer timeouts and flush as needed."""
        while running:
            await asyncio.sleep(buffer_flush_interval)
            try:
                await check_buffer_timeouts(buffer_flush_interval)
            except Exception as e:
                logger.error(f"Error in periodic flush check: {e!s}", exc_info=True)
            if not running:
                break

    logger.info(
        f"Starting periodic buffer flush task (interval: {buffer_flush_interval}s)"
    )
    flush_task = asyncio.create_task(periodic_flush_check())

    logger.info(f"Starting VIN cleanup task (interval: {VIN_CLEANUP_INTERVAL}s)")
    cleanup_task = asyncio.create_task(cleanup_inactive_vins())

    try:
        async for message in consumer.consume():
            if not running:
                break

            try:
                processed_data = await process_message(message)
                if processed_data:
                    await add_to_buffer(processed_data)
                    messages_processed += 1

                    if messages_processed % 1000 == 0:
                        logger.info(f"Processed {messages_processed} messages")

            except Exception as e:
                logger.error(f"Error processing message: {e!s}", exc_info=True)

    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e!s}", exc_info=True)

    finally:
        # Cancel background tasks
        logger.info("Cancelling background tasks...")
        flush_task.cancel()
        cleanup_task.cancel()

        # Wait for tasks to finish cancellation
        with contextlib.suppress(Exception):
            await asyncio.gather(flush_task, cleanup_task, return_exceptions=True)

        # Close consumer
        await consumer.close()
        logger.info("Kafka consumer closed")


async def run_ingestion(
    verbose: bool,
    topic: str,
    group_id: str,
    bootstrap_servers: str,
    auto_offset_reset: str,
    buffer_flush_interval: int,
):
    """
    Configure logging, resolve runtime settings, and launch the Kafka consumer.
    """
    try:
        await setup_logging(verbose=verbose)

        # Validate critical env configuration (raises if missing)
        get_settings()

        await consume_kafka_data(
            topic=topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            buffer_flush_interval=buffer_flush_interval,
        )

    except KeyboardInterrupt:
        logger.info("Keyboard interruption detected")
    except Exception as e:
        logger.error(f"Unhandled error: {e!s}", exc_info=True)
        raise
    finally:
        try:
            await graceful_shutdown()
        except Exception as e:
            logger.error(f"Error during shutdown: {e!s}", exc_info=True)
        shutdown_event.set()
        logger.info("Program terminated")
        logger.debug("Process ending in 1 second...")
        time.sleep(1)


@click.command()
@click.option(
    "--topic",
    type=str,
    default="tesla_V",
    show_default=True,
    envvar=("TESLA_FLEET_KAFKA_TOPIC", "KAFKA_TOPIC"),
    help="Kafka topic to consume",
)
@click.option(
    "--group-id",
    type=str,
    default="tesla-fleet-telemetry-consumer",
    show_default=True,
    envvar=("TESLA_FLEET_KAFKA_GROUP_ID", "KAFKA_GROUP_ID"),
    help="Kafka consumer group ID",
)
@click.option(
    "--bootstrap-servers",
    type=str,
    envvar=("TESLA_FLEET_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"),
    help="Kafka bootstrap servers (comma-separated)",
)
@click.option(
    "--auto-offset-reset",
    type=click.Choice(["earliest", "latest"], case_sensitive=False),
    default="latest",
    show_default=True,
    help="Starting offset position",
)
@click.option(
    "--buffer-flush-interval",
    type=int,
    default=FLUSH_INTERVAL_SECONDS,
    show_default=True,
    help="Buffer flush interval in seconds",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Enable verbose (debug) logging",
)
def cli(
    topic: str | None,
    group_id: str | None,
    bootstrap_servers: str | None,
    auto_offset_reset: str,
    buffer_flush_interval: int,
    verbose: bool,
):
    """
    Tesla Fleet Telemetry ingestion service.
    """
    asyncio.run(
        run_ingestion(
            verbose=verbose,
            topic=topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=auto_offset_reset.lower(),
            buffer_flush_interval=buffer_flush_interval,
        )
    )


if __name__ == "__main__":
    cli()
