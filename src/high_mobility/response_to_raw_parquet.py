"""
This script converts High Mobility json responses into raw parquet time series.
The script operates on S3 buckets. 
"""
import os
from dotenv import load_dotenv
import requests
import json
from urllib.parse import urlencode, quote
from datetime import datetime
from io import BytesIO, StringIO

from rich import print
import boto3

from high_mobility.parse_HighMobility_response import parse_json_obj

def main():
    s3 = boto3.client(
        "s3",
        region_name="fr-par",
        endpoint_url="https://s3.fr-par.scw.cloud",
        aws_access_key_id=os.getenv("S3_KEY") or "",
        aws_secret_access_key=os.getenv("S3_SECRET") or ""
    )
    # s3.list_objects_v2(Bucket="bib-platform-dev-data")
    response = s3.get_object(Bucket="bib-platform-dev-data", Key="renault/response/renault-2024-07-31 15:30:45.275147.json")
    content = json.loads(response["Body"].read().decode('utf-8'))
    # print(content)
    df = parse_json_obj(content)
    dataframe_to_s3(s3, df, "bib-platform-dev-data", "renault/parquet/test.parquet")

def dataframe_to_s3(s3_client, input_datafame, bucket_name, filepath, format="parquet"):
    if format == 'parquet':
        out_buffer = BytesIO()
        input_datafame.to_parquet(out_buffer, index=False)

    elif format == 'csv':
        out_buffer = StringIO()
        input_datafame.to_parquet(out_buffer, index=False)

    s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())


if __name__ == "__main__":
    main()
