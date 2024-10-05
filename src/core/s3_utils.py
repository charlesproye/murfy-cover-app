from typing import Any
from io import BytesIO, StringIO
import os
from dotenv import load_dotenv
import json
import logging

import pyarrow.parquet as pq
import boto3
import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.config import *
from core.pandas_utils import str_split_and_retain_src

load_dotenv()

class S3_Bucket():
    def __init__(self, creds: dict[str, str]=None):

        assert "S3_ENDPOINT" in os.environ, "S3_ENDPOINT varible is not in the environement."
        creds = creds or S3_Bucket.get_creds_from_dot_env()

        self._s3_client = boto3.client(
            "s3",
            region_name="fr-par",
            endpoint_url=os.getenv("S3_ENDPOINT"),
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
        )
        self.bucket_name = creds["bucket_name"]
        self.logger = logging.getLogger("S3_BUCKET")

    @classmethod
    def get_creds_from_dot_env(cls) -> dict[str, str]:
        assert "S3_SECRET" in os.environ, "S3_SECRET varible is not in the environement."
        assert "S3_BUCKET" in os.environ, "S3_BUCKET varible is not in the environement."
        assert "S3_KEY" in os.environ, "S3_KEY varible is not in the environement."

        return {
            "endpoint_url": os.getenv("S3_ENDPOINT"),
            "aws_access_key_id": os.getenv("S3_KEY"),
            "aws_secret_access_key": os.getenv("S3_SECRET"),
            "bucket_name": os.getenv("S3_BUCKET"),
        }

    def save_df_as_parquet(self, pandas_obj:Series|DF, key:str):
        out_buffer = BytesIO()
        pandas_obj.to_parquet(out_buffer)
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

    def list_responses_keys_of_brand(self, brand:str="") -> DF:
        """
        ### Descriptions:
        *Warning:Ayvens x BIB POC specific method. This may not work on other projects.*  
        ### Returns:
        A dataframe where each row represents the key of a response.  
        """
        keys = self.list_keys(f"response/{brand}/")
        keys = keys[keys.str.endswith(".json")]
        if len(keys) == 0:
            self.logger.info(EMTPY_S3_KEYS_WARNING_MSG.format(keys_prefix=brand))
            return DF(None, columns=KEY_LIST_COLUMN_NAMES)
        # Only retain .json responses
        # Reponses are organized as follow response/brand_name/vin/date-of-response.json
        keys = str_split_and_retain_src(keys, "/")
        # Remove files in temp directory
        keys = keys[keys[3] != "temp"]
        keys = keys.drop(columns=[4])
        keys.columns = KEY_LIST_COLUMN_NAMES
        # Check that the file name is a date
        keys['is_valid_file'] = (
            keys["file"]
            .str.split(".", expand=True).loc[:, 0]
            .str.match(r'^\d{4}-\d{2}-\d{2}$')
        )
        keys["is_valid_file"] &= keys["vin"].str.len() != 0
        keys = keys.query(f"is_valid_file")
        
        return keys

    def list_keys(self, key_prefix:str="") -> Series:
        """
        ### Description:
        Returns a pandas Series of the keys present in the bucket.
        ### Parameters:
        key_prefix: A string representing the prefix to filter the keys. If None, all keys will be listed.
        ### Returns:
        A pandas Series of keys present in the bucket.
        """

        # Ensure that the key ends with a /
        if not key_prefix.endswith("/"):
            key_prefix += "/"

        paginator = self._s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=key_prefix)

        keys = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj["Key"] != key_prefix:
                        keys.append(obj['Key'])

        keys_as_series = Series(keys, dtype="string")

        return keys_as_series

    def read_parquet_df(self, key:str) -> DF|Series:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)

        parquet_bytes = response["Body"].read()
        # Convert bytes to a file-like buffer
        parquet_buffer = BytesIO(parquet_bytes)
        
        # Use pyarrow to read the buffer
        table = pq.read_table(parquet_buffer)
        
        # Convert the table to a pandas DataFrame
        df = table.to_pandas()

        return df
    
    def read_key_as_text(self, key:str) -> str:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")

        return object_content
    
    def write_string_into_key(self, content:str, key:str):
        self._s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=content)

    def read_json_file(self, key:str) -> Any:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")
        parsed_object = json.loads(object_content)

        return parsed_object
    

