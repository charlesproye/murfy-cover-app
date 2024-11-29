import boto3
import datetime
import os
from core.s3_utils import S3_Bucket



s3 = S3_Bucket()

bucket_name = os.getenv("S3_BUCKET")
folder_name = 'response/ituran/'  # Ensure this ends with '/'

# Generate a pre-signed POST URL
response = s3.generate_presigned_post(
    key=f'{folder_name}${{filename}}',  # Placeholder for the uploaded file name
    # Fields={"acl": "private"},          # Optional, set file permissions
    # Conditions=[
    #     {"acl": "private"},             # Ensures files are private
    #     ["starts-with", "$Content-Type", ""]  # Allow all content types
    # ],
    expires_in=600000  # Validity period in seconds
)

print("Pre-Signed POST URL:", response['url'])
print("Fields:", response['fields'])
