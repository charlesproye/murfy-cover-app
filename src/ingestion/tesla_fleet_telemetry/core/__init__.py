"""
Core functionality for the tesla-fleet-telemetry module.
"""

from .s3_handler import (
    save_data_to_s3,
    cleanup_old_data,
    get_s3_async_client,
    get_s3_sync_client
)

__all__ = [
    "save_data_to_s3",
    "cleanup_old_data",
    "get_s3_async_client",
    "get_s3_sync_client"
] 
