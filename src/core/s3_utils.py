from functools import lru_cache
import json
import logging
from typing import Annotated, Any
from datetime import datetime
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

import boto3
from fastapi import Depends
import pandas as pd
from pandas import Series
import pyarrow.parquet as pq
from pandas import DataFrame as DF
from pydantic import Field
from pydantic_settings import BaseSettings

from core.config import *
from core.pandas_utils import str_split_and_retain_src


class S3Settings(BaseSettings):
    S3_REGION: str = Field(default="fr-par")
    S3_ENDPOINT: str = Field(default="https://s3.fr-par.scw.cloud")
    S3_BUCKET: str = Field(default=...)
    S3_KEY: str = Field(default=...)
    S3_SECRET: str = Field(default=...)


class S3Service():
    def __init__(self, settings:S3Settings|None = None):
        settings = settings or S3Settings()
        self._s3_client =  boto3.client(
            "s3",
            region_name=settings.S3_REGION,
            endpoint_url=settings.S3_ENDPOINT,
            aws_access_key_id=settings.S3_KEY,
            aws_secret_access_key=settings.S3_SECRET,
        )
        self.bucket_name = settings.S3_BUCKET
        self.logger = logging.getLogger("S3_BUCKET")


    def save_df_as_parquet(self, df:DF, key:str):
        out_buffer = BytesIO()
        df.to_parquet(out_buffer)
        self._s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=out_buffer.getvalue())

    def check_file_exists(self, key: str) -> bool:
        """
        Checks if a file (key) exists in the S3 bucket.

        :param key: The S3 key (path) to check.
        :return: True if the file exists, False otherwise.
        """
        try:
            # Attempt to retrieve metadata of the object
            self._s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True  # If no exception, the key exists
        except Exception as e:
            # Check if the error code is 404, which means the key does not exist
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    # This should be moved else wehere as this is a method specific to the way BIB oragnizes its responses.  
    # Ideally this would be moved to transform.raw_tss.
    def list_responses_keys_of_brand(self, brand:str="") -> DF:
        """
        ### Returns:
        A dataframe where each row represents the key of a response.  
        The columns are key(absolute path), vin, make, file(filename).
        """
        self.logger.debug(f"Listing responses keys of {brand}...")
        keys = self.list_keys(f"response/{brand}/")
        keys = keys[keys.str.endswith(".json")]
        if len(keys) == 0:
            self.logger.info(EMTPY_S3_KEYS_WARNING_MSG.format(keys_prefix=brand))
            return DF(None, columns=KEY_LIST_COLUMN_NAMES)
        # Only retain .json responses
        # Reponses are organized as follow: response/brand_name/vin/date-of-response.json
        keys = str_split_and_retain_src(keys, "/")
        self.logger.debug(f"Keys ending in .json:\n{keys}")
        # Remove files in temp directory
        keys = keys[keys[3] != "temp"]
        self.logger.debug(f"Keys after removing temps:\n{keys}")

        if len(keys.columns) > 5:
            self.logger.debug(f"Keys after last column:\n{keys}")
            keys = keys.drop(columns=[4])
        keys.columns = KEY_LIST_COLUMN_NAMES
        # Check that the file name is a date
        keys['is_valid_file'] = (
            keys["file"]
            .str.split(".", expand=True).loc[:, 0]
            .str.match(r'^\d{4}-\d{2}-\d{2}$')
        )
        keys["is_valid_file"] &= keys["vin"].str.len() != 0
        self.logger.debug(f"set is_valid_file column:\n{keys}")
        keys = keys.query(f"is_valid_file")
        keys["date"] = keys["file"].str[:-5]

        return keys

    def list_keys(self, key_prefix: str = "", max_workers: int = 32) -> Series:
        """
        Returns a pandas Series of the keys present in the bucket.
        Uses multithreading to speed up listing.
        """
        if not key_prefix.endswith("/"):
            key_prefix += "/"

        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=key_prefix)

        keys = []
        
        # Use multithreading to process pages concurrently
        def process_page(page):
            if "Contents" in page:
                return [obj["Key"] for obj in page["Contents"] if obj["Key"] != key_prefix]
            return []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_page, page_iterator))
        
        # Flatten the list
        keys = [key for sublist in results for key in sublist]
        
        return Series(keys, dtype="string") if keys else Series(dtype="string")

    def read_parquet_df(self, key:str, **kwargs) -> DF:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        parquet_bytes = response["Body"].read()                     # Convert bytes to a file-like buffer
        parquet_buffer = BytesIO(parquet_bytes)                     # Use pyarrow to read the buffer
        table = pq.read_table(parquet_buffer, **kwargs)             # Convert the table to a pandas DataFrame
        return table.to_pandas()
    
    def read_csv_df(self, key:str, **kwargs) -> DF:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            return pd.read_csv(response.get("Body"), **kwargs)
        else:
            raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

    def read_key_as_text(self, key:str) -> str:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")

        return object_content

    def read_multiple_json_files(self, keys: list, max_workers=32) -> list:
        """Reads multiple JSON files concurrently using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.read_json_file, keys))
        return results

    def read_json_file(self, key: str):
        """Reads a single JSON file from S3."""
        try:
            response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
            object_content = response["Body"].read().decode("utf-8")
            return json.loads(object_content)
        except Exception as e:
            self.logger.error(f"Failed to read key {key}: {e}")
            return None
    
    def get_last_modified(self, key:str) -> datetime:
        response = self._s3_client.head_object(Bucket=self.bucket_name, Key=key)
        return response["LastModified"]
    
    def list_subfolders(self, prefix: str) -> list[str]:
        """
        Lists immediate subfolders of the given prefix.

        :param prefix: The S3 prefix (path) to list subfolders from.
        :return: A list of subfolder names (without the full path).
        """
        # Ensure the prefix ends with a '/'
        if not prefix.endswith('/'):
            prefix += '/'

        paginator = self._s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')

        subfolders = []
        for page in page_iterator:
            for prefix_dict in page.get('CommonPrefixes', []):
                # Extract just the folder name from the full path
                folder_name = prefix_dict['Prefix'][len(prefix):].rstrip('/')
                if folder_name:  # Ignore empty folder names
                    subfolders.append(folder_name)

        return subfolders

    def generate_presigned_post(self, key:str, conditions:list[Any], expires_in:int=3600) -> dict[str, Any]:
        # conditions = [
        #     {"bucket": self.bucket_name},  # Only allow uploads to this bucket
        #     ["starts-with", "$key", key],  # Restrict to the specific folder
        #     {"acl": "private"},  # Ensure files are private after upload
        # ]
        return self._s3_client.generate_presigned_post(Bucket=self.bucket_name, Key=key, ExpiresIn=expires_in, Conditions=conditions)
    
    def generate_folder_presigned_url(self, folder_name):
        # s3 = S3_Bucket()
        # bucket_name = get_env_var("S3_BUCKET")
        # folder_name = 'response/ituran/'
        
        response = self.generate_presigned_post(
            key=f'{folder_name}${{filename}}',
            expires_in=600000,
            conditions=[
                ["starts-with", "$key", folder_name],
                ["starts-with", "$Content-Type", ""],  # Accepte tous les types de contenu
                {"acl": "private"},  # Fichiers privés
                ["content-length-range", 0, 1000485760],  # Limite la taille des fichiers (ici 10MB)
            ],
        )
        
        return response

    def delete_folder(self, prefix: str, batch_size: int = 1000) -> None:
        """
        Recursively deletes a folder and all its contents from the S3 bucket.
        
        Args:
            prefix: The folder path to delete (e.g., 'my/folder/')
            batch_size: Number of objects to delete in each batch
        """
        # Ensure the prefix ends with a '/'
        if not prefix.endswith('/'):
            prefix += '/'
        
        self.logger.info(f"Starting deletion of folder: {prefix}")
        
        # List all objects in the folder
        paginator = self._s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        
        # Process objects in batches
        current_batch = []
        deleted_count = 0
        
        for page in page_iterator:
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                current_batch.append({'Key': obj['Key']})
            
                # If we've reached the batch size, delete the batch
                if len(current_batch) >= batch_size:
                    self._delete_batch(current_batch)
                    deleted_count += len(current_batch)
                    self.logger.info(f"Deleted {deleted_count} objects from {prefix}")
                    current_batch = []
        
        # Delete any remaining objects
        if current_batch:
            self._delete_batch(current_batch)
            deleted_count += len(current_batch)
            self.logger.info(f"Deleted {deleted_count} objects from {prefix}")
        
        self.logger.info(f"Successfully deleted folder {prefix} and all its contents")

    def _delete_batch(self, objects: list[dict[str, str]]) -> None:
        """
        Deletes a batch of objects from the S3 bucket.
        
        Args:
            objects: List of objects to delete, each in the format {'Key': 'path/to/object'}
        """
        self._s3_client.delete_objects(
            Bucket=self.bucket_name,
            Delete={
                'Objects': objects,
                'Quiet': True
            }
        )

    def store_object(self, object: bytes, filename: str):
        self._s3_client.put_object(
            Body=object,
            Bucket=self.bucket_name,
            Key=filename,
        )

@lru_cache
def get_s3():
    return S3Service()

S3Dep = Annotated[S3Service,Depends(get_s3)]
