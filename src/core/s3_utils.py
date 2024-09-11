from typing import Any
import os
from io import BytesIO, StringIO
from dotenv import load_dotenv
import cbor2
import json

import pyarrow.parquet as pq

import boto3
import pandas as pd
from pandas import DataFrame as DF
from pandas import Series
from rich import print

load_dotenv()

class S3_Bucket():
    def __init__(self):
        assert "S3_ENDPOINT" in os.environ, "S3_ENDPOINT varible is not in the environement."
        assert "S3_SECRET" in os.environ, "S3_SECRET varible is not in the environement."
        assert "S3_BUCKET" in os.environ, "S3_BUCKET varible is not in the environement."
        assert "S3_KEY" in os.environ, "S3_KEY varible is not in the environement."

        self._s3_client = boto3.client(
            "s3",
            region_name="fr-par",
            endpoint_url=os.getenv("S3_ENDPOINT"),
            aws_access_key_id=os.getenv("S3_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET"),
        )

        self.bucket_name = os.getenv("S3_BUCKET")

    def save_df_as_parquet(self, pandas_obj:Series|DF, key:str):
        out_buffer = BytesIO()
        pandas_obj.to_parquet(out_buffer)
        # Ensure that key ends with .parquet
        # if not key.endswith(".parquet"):
        #     key += ".parquet"
        self._s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=out_buffer.getvalue())

    def list_keys(self, key_prefix:str="") -> list[str]:
        """
        ### Description:
        Returns a list of the keys present in the bucket.
        ### Parameters:
        key_prefix: A string representing the prefix to filter the keys. If None, all keys will be listed.
        ### Returns:
        A list of keys present in the bucket.
        """
        paginator = self._s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=key_prefix)

        # Ensure that the key ends with a /
        if not key_prefix.endswith("/"):
            key_prefix += "/"

        keys = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj["Key"] != key_prefix:
                        keys.append(obj['Key'])

        return keys

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
    
    def read_json_file(self, key:str) -> Any:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")
        parsed_object = json.loads(object_content)

        return parsed_object
    
    def read_cbor(self, key:str):
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read()
        obj = cbor2.loads(object_content)

        return obj

