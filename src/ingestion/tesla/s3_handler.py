import os
import json
import logging
from datetime import datetime, timedelta
import aioboto3
import asyncio

async def compress_data():
    session = aioboto3.Session()
    async with session.client(
        's3',
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET"),
    ) as s3:
        bucket_name = os.getenv("S3_BUCKET")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # List all vehicle folders
            vehicle_folders = await s3.list_objects_v2(Bucket=bucket_name, Prefix="response/tesla/", Delimiter='/')
            for folder in vehicle_folders.get('CommonPrefixes', []):
                vehicle_id = folder.get('Prefix').split('/')[-2]
                temp_folder = f"response/tesla/{vehicle_id}/temp/"
                
                # List all files in temp folder
                temp_files = await s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_folder)
                
                data_by_date = {}
                for obj in temp_files.get('Contents', []):
                    file_key = obj['Key']
                    response = await s3.get_object(Bucket=bucket_name, Key=file_key)
                    file_content = await response['Body'].read()
                    file_data = json.loads(file_content.decode('utf-8'))
                    
                    # Extract only the date part (YYYY-MM-DD) from readable_date
                    readable_date = file_data.get('readable_date', yesterday)
                    file_date = readable_date.split()[0]  # This will take only the date part
                    
                    if file_date not in data_by_date:
                        data_by_date[file_date] = []
                    data_by_date[file_date].append(file_data)
                    
                    # Delete the temp file
                    await s3.delete_object(Bucket=bucket_name, Key=file_key)
                
                # Save compressed data for each date
                for date, all_data in data_by_date.items():
                    compressed_file_key = f"response/tesla/{vehicle_id}/{date}.json"
                    await s3.put_object(
                        Bucket=bucket_name,
                        Key=compressed_file_key,
                        Body=json.dumps(all_data, indent=2),
                        ContentType='application/json'
                    )
                    
                    logging.info(f"Data compressed and saved for vehicle {vehicle_id} on {date}")
        except Exception as e:
            logging.error(f"Error during compression: {str(e)}")
            raise

async def save_data_to_s3(data, vehicle_id):
    session = aioboto3.Session()
    async with session.client(
        's3',
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET"),
    ) as s3:
        bucket_name = os.getenv("S3_BUCKET")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_key = f"response/tesla/{vehicle_id}/temp/{timestamp}.json"

        try:
            await s3.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            logging.info(f"Data saved to S3 successfully for {vehicle_id}")
        except Exception as e:
            logging.error(f"Error writing to S3: {str(e)}")
