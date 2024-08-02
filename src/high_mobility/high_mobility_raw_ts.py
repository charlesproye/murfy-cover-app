"""
This script converts High Mobility responses into raw parquet time series.
The script operates on S3 buckets. 
"""
import logging

from rich import print
from rich.traceback import install as install_rich_traceback
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF

from high_mobility.high_mobility_response_parsing import parse_response_as_df
from core.s3_utils import S3_Bucket

logger = logging.getLogger(__name__)

def main():
    install_rich_traceback(extra_lines=0, width=130)
    bucket = S3_Bucket()
    
    # Get list of objects in response key
    keys = Series(bucket.list_keys("response"))
    if len(keys) == 0:
        logger.info("""
            No responses found in the 'response' folder.
            No raw time series have been generated.
        """)
        return
    # Only retain .cbor responses
    keys = keys[keys.str.endswith(".cbor")]
    # Reponses are organized as follow response/brand_name/vin/response_timestamp.cbor
    # We extract the brand and vin
    keys = pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns") 
    keys.columns = ["key", "brand", "vin", "file"] # Set column names
    # for each group of responses for a vin create a raw time series parquet
    keys.groupby(["brand", "vin"]).apply(parse_responses_as_raw_ts, bucket, include_groups=False)

def parse_responses_as_raw_ts(src_keys:DF, bucket:S3_Bucket):
    raw_jsons:Series = src_keys["key"].apply(bucket.read_cbor)                      # Read responses
    raw_df:DF = pd.concat([parse_response_as_df(raw_json) for raw_json in raw_jsons])   # Parse and concat them into a single df 
    dest_key = "/".join(["raw_ts", "time_series", *src_keys.name]) + ".parquet"                   # Create path to save the raw ts
    bucket.save_pandas_obj_as_parquet(raw_df, dest_key)                             # save the raw ts


if __name__ == "__main__":
    main()
