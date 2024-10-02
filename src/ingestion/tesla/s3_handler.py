import os
import json
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime, timedelta
import asyncio

async def save_data_to_s3(data, vehicle_id):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _save_data_to_s3, data, vehicle_id)

def _save_data_to_s3(data, vehicle_id):
    s3 = boto3.client(
        's3',
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET"),
    )
    bucket_name = os.getenv("S3_BUCKET")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_key = f"response/tesla/{vehicle_id}/temp/{timestamp}.json"

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logging.info(f"Data saved to S3 successfully for {vehicle_id}")
    except ClientError as e:
        logging.error(f"Error writing to S3: {e}")

async def compress_data():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _compress_data)

def _compress_data():
    s3 = boto3.client(
        's3',
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET"),
    )
    bucket_name = os.getenv("S3_BUCKET")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # List all vehicle folders
    vehicle_folders = s3.list_objects_v2(Bucket=bucket_name, Prefix="response/tesla/", Delimiter='/')
    for folder in vehicle_folders.get('CommonPrefixes', []):
        vehicle_id = folder.get('Prefix').split('/')[-2]
        temp_folder = f"response/tesla/{vehicle_id}/temp/"
        
        # List all files in temp folder
        temp_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_folder)
        
        all_data = []
        for obj in temp_files.get('Contents', []):
            file_key = obj['Key']
            file_content = s3.get_object(Bucket=bucket_name, Key=file_key)['Body'].read().decode('utf-8')
            all_data.append(json.loads(file_content))
            
            # Delete the temp file
            s3.delete_object(Bucket=bucket_name, Key=file_key)
        
        # Save compressed data
        compressed_file_key = f"response/tesla/{vehicle_id}/{yesterday}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=compressed_file_key,
            Body=json.dumps(all_data, indent=2),
            ContentType='application/json'
        )
        
        logging.info(f"Data compressed and saved for vehicle {vehicle_id} on {yesterday}")
