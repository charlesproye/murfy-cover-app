"""
External API S3 service using centralized S3Service.

This module provides S3 functionality for the external API,
using the centralized S3Service from core for security and consistency.
"""

from functools import lru_cache

from botocore.exceptions import ClientError
from fastapi import HTTPException

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from db_models.enums import AssetTypeEnum
from external_api.core.config import settings


@lru_cache
def _get_s3_service() -> S3Service:
    """Get or create the S3Service instance (singleton pattern)."""
    s3_settings = S3Settings(
        S3_ENDPOINT=settings.S3_ENDPOINT,
        S3_REGION=getattr(
            settings, "S3_REGION", "fr-par"
        ),  # Default to fr-par if not set
        S3_BUCKET=settings.S3_BUCKET_ASSETS,
        S3_KEY=settings.S3_KEY,
        S3_SECRET=settings.S3_SECRET,
    )
    return S3Service(s3_settings)


# Backward compatibility: expose underlying boto3 client
# Some code expects to import s3_client directly
@lru_cache
def _get_s3_client():
    """Get the underlying boto3 S3 client for backward compatibility."""
    return _get_s3_service()._s3_client


# Export for backward compatibility
s3_client = _get_s3_client()


# GET FILE URLS
# --------------------------------
def get_file_url(file_full_path: str) -> str:
    """
    Generate a presigned URL for downloading a file from S3.

    Args:
        file_full_path: S3 key (path) to the file

    Returns:
        Presigned URL string

    Raises:
        Exception: If URL generation fails
    """
    try:
        s3 = _get_s3_service()
        # Use the underlying boto3 client for presigned URL generation
        return s3._s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3.bucket_name, "Key": file_full_path},
            ExpiresIn=3600,
        )
    except Exception as e:
        raise Exception(f"Error getting file_full_path signed url: {e}") from e


def get_model_image_url(image_name: str):
    return get_file_url(f"{AssetTypeEnum.car_images.value}/{image_name}")


def get_make_image_url(image_name: str):
    return get_file_url(f"{AssetTypeEnum.make_images.value}/{image_name}")


# UPLOAD FILES
# --------------------------------
def upload_file_to_s3(file_path: str, file_content: bytes):
    """
    Upload a file to S3.

    Args:
        file_path: S3 key (path) where the file should be stored
        file_content: File content as bytes

    Returns:
        Response from S3

    Raises:
        HTTPException: If upload fails
    """
    try:
        s3 = _get_s3_service()
        s3.store_object(file_content, file_path)
        # Return a success response for backward compatibility
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Error uploading file to s3: {e!s}"
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error uploading file to s3: {e!s}"
        ) from e


# DELETE FILES
# --------------------------------
def delete_file_in_s3(full_path: str):
    """
    Delete a file from S3.

    Args:
        full_path: S3 key (path) to the file to delete

    Raises:
        HTTPException: If deletion fails
    """
    try:
        s3 = _get_s3_service()
        # Use the underlying boto3 client for deletion
        s3._s3_client.delete_object(Bucket=s3.bucket_name, Key=full_path)
    except ClientError as e:
        raise HTTPException(
            status_code=500, detail=f"Error deleting file in s3: {e!s}"
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error deleting file in s3: {e!s}"
        ) from e
