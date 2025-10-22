import boto3
from fastapi import HTTPException

from external_api.core.config import settings

# INITIALISATION
# --------------------------------

S3_KEY_FORM = settings.S3_KEY
S3_SECRET_FORM = settings.S3_SECRET
S3_ENDPOINT = settings.S3_ENDPOINT
S3_BUCKET = settings.S3_BUCKET

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY_FORM,
    aws_secret_access_key=S3_SECRET_FORM,
)


# GET FILE URLS
# --------------------------------
def get_file_url(file_full_path: str):
    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": file_full_path},
            ExpiresIn=3600,
        )
    except Exception as e:
        raise Exception(f"Error getting file_full_path signed url: {e}") from e


# UPLOAD FILES
# --------------------------------
def upload_file_to_s3(file_path, file_content):
    try:
        response = s3_client.put_object(
            Bucket=S3_BUCKET, Key=file_path, Body=file_content
        )
        # Check if the upload was successful by looking for HTTPStatusCode 200
        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return response
        else:
            raise HTTPException(status_code=500, detail="Failed to upload file to S3")

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error uploading file to s3: {e!s}"
        ) from e


# DELETE FILES
# --------------------------------
def delete_file_in_s3(full_path: str):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=full_path)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error deleting file in s3: {e!s}"
        ) from e

