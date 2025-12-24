"""FastAPI dependencies for external API."""

from functools import lru_cache

import aiohttp

from core.s3.async_s3 import AsyncS3
from external_api.core.http_client import HTTP_CLIENT


@lru_cache(maxsize=1)
def get_s3_client() -> AsyncS3:
    """
    Get S3 client singleton.

    This function is cached to ensure only one S3 client instance is created
    throughout the application lifecycle. The bucket should be specified
    when calling upload/download methods.

    Returns:
        AsyncS3 client instance
    """
    return AsyncS3()


@lru_cache(maxsize=1)
def get_s3_client_fast() -> AsyncS3:
    """
    Get S3 client singleton optimized for fast uploads.

    This client has checksum validation disabled for faster upload speeds.
    Use this for uploading generated files (like PDFs) where integrity is
    less critical or verified elsewhere.

    Returns:
        AsyncS3 client instance with disabled checksums
    """
    return AsyncS3(disable_checksum=True)


def get_http_client() -> aiohttp.ClientSession:
    """
    Get the shared aiohttp client session.

    This returns the singleton HTTP client that's managed by the FastAPI lifespan.
    The session is started on application startup and closed on shutdown.

    Returns:
        aiohttp.ClientSession instance
    """
    return HTTP_CLIENT()
