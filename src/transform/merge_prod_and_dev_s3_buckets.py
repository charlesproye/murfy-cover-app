import logging
from datetime import datetime as DT
from datetime import timedelta as TD
from dateutil import parser
from dotenv import load_dotenv
import os
from time import sleep

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from apscheduler.triggers.interval import IntervalTrigger

from core.s3_utils import S3_Bucket
from jobs.high_mobility.constants import *
from jobs.base_jobs.job_interval import Jobinterval
from core.config import *
from core.time_series_processing import process_date, estimate_dummy_soh

load_dotenv()

DEV_CREDS = {
    "bucket_name":os.getenv("DEV_S3_BUCKET"),
    "aws_access_key_id":os.getenv("DEV_S3_KEY"),
    "aws_secret_access_key":os.getenv("DEV_S3_SECRET"),
}
PROD_CREDS = {
    "bucket_name":os.getenv("PROD_S3_BUCKET"),
    "aws_access_key_id":os.getenv("PROD_S3_KEY"),
    "aws_secret_access_key":os.getenv("PROD_S3_SECRET"),
}


def main():
    dev_bucket = S3_Bucket(DEV_CREDS)
    prod_bucket = S3_Bucket(PROD_CREDS)
    
    for brand in HM_HANDLED_BRANDS:
        print("=============", brand)
        keys = Series(dev_bucket.list_keys(f"response/{brand}/"), dtype="string") # Make sure to leave the / at the end
        # print(keys)
        if len(keys) == 0:
            print("""
                No responses found in the 'response' folder.
                No raw time series have been generated.
            """)
            continue
        # print(keys)
        # Only retain .json responses
        keys = keys[keys.str.endswith(".json")]
        # Reponses are organized as follow response/brand_name/vin/date-of-response.json
        # We extract the brand and vin
        keys = pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns").iloc[:, 0:-1]
        keys.columns = ["key", "brand", "vin", "file"] # Set column names
        # Check that the file name is a date
        keys['is_valid_file'] = (
            keys['file']
            .str.split(".", expand=True).loc[:, 0]
            .str.match(r'^\d{4}-\d{2}-\d{2}$')
        )
        keys["is_valid_file"] &= keys["vin"].str.len() != 0
        keys = keys.query(f"is_valid_file")
        keys["dest_key"] = keys["key"]
        print(keys)
        move_keys_to_prod(prod_bucket, dev_bucket, keys)

def move_keys_to_prod(prod_bucket: S3_Bucket, dev_bucket: S3_Bucket, keys:DF):
    for idx, key_info in keys.iterrows():
        print("moving", key_info["key"])
        reponse = dev_bucket.read_key_as_text(key_info["key"])
        # print("response:", reponse)
        prod_bucket.write_string_into_key(reponse, key_info["dest_key"])
        print("====================")
        # sleep(1)

if __name__ == "__main__":
    main()
