import os
import json
import logging
from datetime import datetime, timedelta
import aioboto3
import boto3
import re
from botocore.exceptions import ClientError
import asyncio
import random

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
            vehicle_folders = await s3.list_objects_v2(Bucket=bucket_name, Prefix="response/tesla/", Delimiter='/')
            if not vehicle_folders.get('CommonPrefixes'):
                logging.info("No vehicle folders found to compress")
                return
                
            compression_tasks = []
            
            for folder in vehicle_folders['CommonPrefixes']:
                vehicle_id = folder.get('Prefix', '').split('/')[-2]
                if vehicle_id:
                    task = asyncio.create_task(compress_vehicle_data(s3, bucket_name, vehicle_id, yesterday))
                    compression_tasks.append(task)
            
            if compression_tasks:
                # Exécuter les compressions en parallèle avec une limite de concurrence
                await asyncio.gather(*compression_tasks)
            else:
                logging.info("No compression tasks created")

        except Exception as e:
            logging.error(f"Error during compression: {str(e)}")
            raise

async def compress_vehicle_data(s3, bucket_name, vehicle_id, yesterday):
    temp_folder = f"response/tesla/{vehicle_id}/temp/"
    
    try:
        temp_files = await s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_folder)
        
        if 'Contents' not in temp_files or not temp_files['Contents']:
            logging.info(f"No temp files to compress for vehicle {vehicle_id}")
            return
        
        data_by_date = {}
        files_to_delete = []  # Liste des fichiers à supprimer
        
        for obj in temp_files['Contents']:
            try:
                file_key = obj['Key']
                response = await s3.get_object(Bucket=bucket_name, Key=file_key)
                file_content = await response['Body'].read()
                
                try:
                    file_data = json.loads(file_content.decode('utf-8'))
                except json.JSONDecodeError:
                    logging.error(f"Corrupted JSON in file {file_key}, deleting file")
                    files_to_delete.append(file_key)
                    continue
                
                if not isinstance(file_data, dict):
                    logging.error(f"Invalid data format in file {file_key}, deleting file")
                    files_to_delete.append(file_key)
                    continue
                
                readable_date = file_data.get('readable_date')
                if not readable_date:
                    try:
                        file_timestamp = os.path.basename(file_key).split('.')[0]
                        if len(file_timestamp) == 14:  # Format: YYYYMMDDHHMMSS
                            file_datetime = datetime.strptime(file_timestamp, '%Y%m%d%H%M%S')
                        else:  # Format: YYYYMMDDHHMMSSffffff
                            file_datetime = datetime.strptime(file_timestamp[:14], '%Y%m%d%H%M%S')
                        
                        readable_date = file_datetime.strftime('%Y-%m-%d %H:%M:%S')
                        file_data['readable_date'] = readable_date
                    except (ValueError, IndexError):
                        logging.error(f"Invalid timestamp in filename {file_key}, deleting file")
                        files_to_delete.append(file_key)
                        continue
                
                file_date = readable_date.split()[0]
                
                if file_date not in data_by_date:
                    data_by_date[file_date] = []
                data_by_date[file_date].append(file_data)
                files_to_delete.append(file_key)  # Ajouter à la liste des fichiers à supprimer
                
            except Exception as e:
                logging.error(f"Error processing file {obj.get('Key', 'unknown')}, deleting file: {str(e)}")
                files_to_delete.append(obj['Key'])
                continue
        
        if not data_by_date:
            logging.warning(f"No valid data to compress for vehicle {vehicle_id}")
            # Supprimer les fichiers invalides même s'il n'y a pas de données valides
            if files_to_delete:
                delete_tasks = [s3.delete_object(Bucket=bucket_name, Key=key) for key in files_to_delete]
                await asyncio.gather(*delete_tasks)
            return
            
        # Sauvegarder les données compressées et supprimer les fichiers temporaires
        tasks = []
        
        # Ajouter les tâches de sauvegarde
        for date, data in data_by_date.items():
            if data:
                tasks.append(
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=f"response/tesla/{vehicle_id}/{date}.json",
                        Body=json.dumps(data),
                        ContentType='application/json'
                    )
                )
        
        # Ajouter les tâches de suppression
        for file_key in files_to_delete:
            tasks.append(s3.delete_object(Bucket=bucket_name, Key=file_key))
        
        if tasks:
            await asyncio.gather(*tasks)
            logging.info(f"Successfully compressed data for vehicle {vehicle_id}")
        
    except Exception as e:
        logging.error(f"Error compressing data for vehicle {vehicle_id}: {str(e)}")

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
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        file_key = f"response/tesla/{vehicle_id}/temp/{timestamp}.json"

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                await s3.put_object(
                    Bucket=bucket_name,
                    Key=file_key,
                    Body=json.dumps(data, indent=2),
                    ContentType='application/json'
                )
                logging.info(f"Data saved to S3 successfully for {vehicle_id}")
                return
            except Exception as e:
                if "OperationAborted" in str(e):
                    retry_count += 1
                    if retry_count < max_retries:
                        await asyncio.sleep(random.uniform(0.1, 0.5))
                        continue
                logging.error(f"Error writing to S3: {str(e)}")
                raise
