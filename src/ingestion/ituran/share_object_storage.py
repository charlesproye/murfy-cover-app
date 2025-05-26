import boto3
import datetime
import os
from core.s3_utils import S3Service
import requests



s3 = S3Service()

bucket_name = os.getenv("S3_BUCKET")
folder_name = 'response/ituran/'  # Ensure this ends with '/'

# Generate a pre-signed POST URL
response = s3.generate_folder_presigned_url(folder_name)
print("Pre-Signed POST URL:", response['url'])
print("Fields:", response['fields'])
