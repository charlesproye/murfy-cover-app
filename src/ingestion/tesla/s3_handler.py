import os
import json
import logging
from datetime import datetime, timedelta
import aioboto3
import boto3
import re
from botocore.exceptions import ClientError
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

async def consolidate_all_tesla_files():
    session = aioboto3.Session()
    async with session.client(
        's3',
        region_name=os.getenv("S3_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET"),
    ) as s3:
        bucket_name = os.getenv("S3_BUCKET")
        tesla_prefix = "response/tesla/"

        try:
            response = await s3.list_objects_v2(Bucket=bucket_name, Prefix=tesla_prefix, Delimiter='/')
            
            for folder in response.get('CommonPrefixes', []):
                vin = folder.get('Prefix').split('/')[-2]
                logging.info(f"Consolidating files for VIN: {vin}")
                await consolidate_files_for_vin(s3, bucket_name, vin)

        except Exception as e:
            logging.error(f"Error listing Tesla folders: {str(e)}")

async def consolidate_files_for_vin(s3, bucket_name, vin):
    consolidated_data = {}
    prefix = f"response/tesla/{vin}/"

    try:
        logging.info(f"Starting consolidation for VIN {vin}")
        logging.info(f"Using prefix: {prefix}")

        # List all Tesla objects
        tesla_prefix = "response/tesla/"

        # Now list objects for the specific VIN
        response = await s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        logging.info(f"Response keys for VIN {vin}: {response.keys()}")
        if 'Contents' in response:
            logging.info(f"Number of objects found for VIN {vin}: {len(response['Contents'])}")
        else:
            logging.warning(f"No 'Contents' key in response for VIN {vin}")

        for obj in response.get('Contents', []):
            key = obj['Key']
            logging.info(f"Processing file: {key}")
            if key.endswith('.json') and '/temp/' not in key:
                filename = os.path.basename(key)
                date_str = extract_date(filename)
                if not date_str:
                    logging.warning(f"Skipping file with invalid date format: {key}")
                    continue

                try:
                    file_response = await s3.get_object(Bucket=bucket_name, Key=key)
                    file_content = await file_response['Body'].read()
                    data = json.loads(file_content.decode('utf-8'))

                    if date_str not in consolidated_data:
                        consolidated_data[date_str] = []
                    
                    consolidated_data[date_str].extend(flatten_list(data))
                    logging.info(f"Data from {key} added to consolidated data for {date_str}")

                except Exception as e:
                    logging.error(f"Error reading file {key}: {str(e)}")

        # Sauvegarde les données consolidées
        for date_str, data_list in consolidated_data.items():
            consolidated_filename = f"{prefix}{date_str}.json"
            consolidated_content = json.dumps(data_list, indent=2)
            try:
                await s3.put_object(Bucket=bucket_name, Key=consolidated_filename, Body=consolidated_content)
                logging.info(f"Consolidated file created: {consolidated_filename}")
            except Exception as e:
                logging.error(f"Error saving consolidated file {consolidated_filename}: {str(e)}")

        # Supprime tous les fichiers sauf les consolidés et ceux dans le dossier temp
        logging.info(f"Checking files for deletion in {prefix}")
        for obj in response.get('Contents', []):
            key = obj['Key']
            logging.info(f"Examining file for deletion: {key}")
            if key.endswith('.json') and '/temp/' not in key:
                filename = os.path.basename(key)
                logging.info(f"Checking filename: {filename}")
                if not re.match(r'^\d{4}-\d{2}-\d{2}\.json$', filename):
                    try:
                        await s3.delete_object(Bucket=bucket_name, Key=key)
                        logging.info(f"File deleted: {key}")
                    except Exception as e:
                        logging.error(f"Error deleting file {key}: {str(e)}")
                else:
                    logging.info(f"File kept: {key}")
            else:
                logging.info(f"Skipping file: {key}")

        logging.info(f"Consolidation completed for VIN {vin}")

    except Exception as e:
        logging.error(f"Error in consolidate_files_for_vin for VIN {vin}: {str(e)}")


def extract_date(filename):
    # Essaie différents formats de date
    patterns = [
        r'(\d{4}-\d{2}-\d{2})',  # Format YYYY-MM-DD
        r'(\d{4}\d{2}\d{2})',    # Format YYYYMMDD
        r'(\d{8})'               # Autre format de 8 chiffres pour la date
    ]
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            date_str = match.group(1)
            # Convertit en format YYYY-MM-DD si nécessaire
            if '-' not in date_str:
                try:
                    date_obj = datetime.strptime(date_str, '%Y%m%d')
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return date_str
    return None

def flatten_list(data):
    if isinstance(data, list):
        return [item for sublist in data for item in (flatten_list(sublist) if isinstance(sublist, list) else [sublist])]
    return [data]

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
