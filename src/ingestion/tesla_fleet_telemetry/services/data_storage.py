"""
Tesla Fleet Telemetry data storage operations using core S3.

This module provides Tesla-specific business logic for saving and cleaning up
telemetry data, using the centralized S3 client from core.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any

from botocore.client import Config

from core.s3.async_s3 import AsyncS3
from core.s3.settings import S3Settings
from ingestion.tesla_fleet_telemetry.config.settings import get_settings

logger = logging.getLogger("tesla-data-storage")

# Global Tesla S3 client
_tesla_s3_client: AsyncS3 | None = None


def _get_tesla_s3_client() -> AsyncS3:
    """
    Get Tesla-specific S3 client (singleton pattern).

    Returns:
        AsyncS3 configured for Tesla Fleet Telemetry
    """
    global _tesla_s3_client

    if _tesla_s3_client is None:
        tesla_settings = get_settings()

        # Convert Tesla settings to S3Settings
        s3_settings = S3Settings(
            S3_ENDPOINT=tesla_settings.s3_endpoint,
            S3_REGION=tesla_settings.s3_region,
            S3_BUCKET=tesla_settings.s3_bucket,
            S3_KEY=tesla_settings.s3_key,
            S3_SECRET=tesla_settings.s3_secret,
        )

        # Tesla-specific boto config (disable checksum validation for compatibility)
        tesla_config = Config(
            signature_version="s3v4",
            s3={
                "addressing_style": "path",
                "payload_signing_enabled": False,
                "use_accelerate_endpoint": False,
                "checksum_validation": False,  # Disable for Tesla compatibility
                "use_dualstack_endpoint": False,
            },
            connect_timeout=5,
            read_timeout=60,
            retries={"max_attempts": 3, "mode": "standard"},
            parameter_validation=True,
            max_pool_connections=200,
        )

        _tesla_s3_client = AsyncS3(
            settings=s3_settings,
            custom_config=tesla_config,
            max_concurrency=200,
        )

    return _tesla_s3_client


async def cleanup_client():
    """Clean up the Tesla S3 client."""
    global _tesla_s3_client
    _tesla_s3_client = None
    logger.info("Tesla S3 client cleaned up")


# Backward compatibility alias
cleanup_clients = cleanup_client


async def save_data_to_s3(data: list[dict[str, Any]], vin: str) -> bool:
    """
    Save telemetry data to S3 temp folder.

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
    s3_client = _get_tesla_s3_client()

    # Organize data by hour to group writes
    data_by_hour = {}
    for item in data:
        readable_date = item.get("readable_date", "")
        if readable_date:
            try:
                hour_str = datetime.strptime(
                    readable_date, "%Y-%m-%d %H:%M:%S"
                ).strftime("%Y-%m-%d_%H")
                if hour_str not in data_by_hour:
                    data_by_hour[hour_str] = []
                data_by_hour[hour_str].append(item)
            except Exception:
                current_hour = datetime.now().strftime("%Y-%m-%d_%H")
                if current_hour not in data_by_hour:
                    data_by_hour[current_hour] = []
                data_by_hour[current_hour].append(item)
        else:
            current_hour = datetime.now().strftime("%Y-%m-%d_%H")
            if current_hour not in data_by_hour:
                data_by_hour[current_hour] = []
            data_by_hour[current_hour].append(item)

    # Write one file per hour
    success = True
    files_to_upload = {}

    for hour_str, hour_data in data_by_hour.items():
        key = f"{settings.base_s3_path}/{vin}/temp/{hour_str}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"
        body = json.dumps(hour_data).encode("utf-8")
        files_to_upload[key] = body

    if files_to_upload:
        results = await s3_client.upload_files(files_to_upload)
        errors = [result for result in results if isinstance(result, Exception)]
        success = not errors and all(result is None for result in results)

        if errors:
            logger.error(
                "Errors saving data: %s",
                [str(result) for result in errors],
            )

    return success


async def cleanup_old_data(retention_days: int = 30) -> bool:
    """
    Clean up old data beyond the retention period.

    Args:
        retention_days: Number of days to retain data

    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    logger.info(f"Starting cleanup of data older than {retention_days} days")

    settings = get_settings()
    s3_client = _get_tesla_s3_client()
    cutoff_date = datetime.now() - timedelta(days=retention_days)

    try:
        folders, _ = await s3_client.list_content(f"{settings.base_s3_path}/")

        if not folders:
            logger.info("No vehicles found for cleanup")
            return True

        cleanup_tasks = []
        for folder in folders:
            vin = folder.rstrip("/").split("/")[-1]
            if vin and vin != "temp":
                task = asyncio.create_task(
                    _cleanup_vehicle_data(s3_client, vin, cutoff_date)
                )
                cleanup_tasks.append(task)

        if not cleanup_tasks:
            logger.info("No cleanup tasks created")
            return True

        results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))

        logger.info(
            f"Cleanup completed: {success_count} vehicles processed successfully, {error_count} errors"
        )

        return error_count == 0

    except Exception as e:
        logger.error(f"Error during data cleanup: {e!s}")
        return False


async def _cleanup_vehicle_data(
    s3_client: AsyncS3, vin: str, cutoff_date: datetime
) -> bool:
    """Clean up old data for a specific vehicle."""
    settings = get_settings()
    vehicle_prefix = f"{settings.base_s3_path}/{vin}/"

    try:
        _, files = await s3_client.list_content(vehicle_prefix)

        # Also list content of temp folder
        _, temp_files = await s3_client.list_content(f"{vehicle_prefix}temp/")

        all_files = files + temp_files

        if not all_files:
            return True

        files_to_delete = []

        for file_path in all_files:
            try:
                filename = file_path.split("/")[-1]
                if not filename.endswith(".json"):
                    continue

                # Parse timestamp from filename
                # Expected formats:
                # Temp files: YYYY-MM-DD_HH_YYYYMMDDHHMMSSuuuuuu.json (from save_data_to_s3)
                # Regular files: YYYY-MM-DD.json (implied)

                date_str = ""
                if "_20" in filename:  # Likely a temp file with timestamp appended
                    date_str = filename.split("_")[0]  # Extract YYYY-MM-DD part
                else:
                    date_str = filename[:-5]

                try:
                    file_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    # Try parsing with hour if simple date fails (though split above handles it for temp)
                    continue

                if file_date < cutoff_date:
                    files_to_delete.append(file_path)

            except (ValueError, IndexError) as e:
                logger.warning(f"Unable to parse date from file {file_path}: {e!s}")
                continue

        if not files_to_delete:
            logger.debug(f"No files to delete for vehicle {vin}")
            return True

        # Delete in batches
        batch_size = 100
        for i in range(0, len(files_to_delete), batch_size):
            batch = files_to_delete[i : i + batch_size]
            delete_tasks = [s3_client.delete_file(key) for key in batch]
            await asyncio.gather(*delete_tasks)

        logger.info(
            f"Cleanup successful for vehicle {vin}: {len(files_to_delete)} files deleted"
        )
        return True

    except Exception as e:
        logger.error(f"Error cleaning up data for vehicle {vin}: {e!s}")
        return False
