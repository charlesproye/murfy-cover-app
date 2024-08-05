"""
This script converts High Mobility responses into raw parquet time series.
The script operates on S3 buckets. 
"""
import logging
from typing import Any
from datetime import datetime as DT

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF

from core.s3_utils import S3_Bucket
from core.console_utils import main_decorator

logger = logging.getLogger(__name__)

@main_decorator
def main():
    logging.basicConfig(level=logging.INFO)

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
    raw_jsons:Series = src_keys["key"].apply(bucket.read_cbor)                          # Read responses
    raw_df:DF = pd.concat([parse_response_as_df(raw_json) for raw_json in raw_jsons])   # Parse and concat them into a single df 
    dest_key = "/".join(["raw_ts", "time_series", *src_keys.name]) + ".parquet"         # Create path to save the raw ts
    print(src_keys.name)
    print(raw_df)
    bucket.save_pandas_obj_as_parquet(raw_df, dest_key)                                 # save the raw ts

def parse_response_as_df(src) -> DF:
    flatten_dict = flatten_json_obj(src, {})
    print(flatten_dict)
    df = DF.from_dict(flatten_dict, orient="index").pipe(set_date)
    print(df)
    print("=============")

    return df

def set_date(df:DF) -> DF:
    if df.empty:
        return df.assign(date=pd.Series(dtype='datetime64[s]'))
    date = pd.to_datetime(df.index.to_series()).dt.as_unit("s")
    df["date"] = date
    df.index = date
    df.index.name = "date"

    return df

def flatten_json_obj(src:dict, dst:dict, timestamp=None, path:list[str]=[]) -> dict[Any,dict[str,Any]]:
    """
    ### Description:
    Recursively search for and set store values.  
    The values are stored in a dict that uses the timestamps as key to avoid having duplicate timestamps.   
    ### Parameters:
    - src: response dictionnary
    - dst:
        Must be empty when called for the first time. 
        IDK why but a default value would not reset which led to a bug where raw dataframes would retain the value of previous responses.  
    Don't fill timestamp and path either.
    ### Returns:
    dict of the values indexed by timestamp.
    """
    if "timestamp" in src:
        timestamp = DT.strptime(src["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")

    dst = try_add_to_dst(src, dst, timestamp, path, "data")
    dst = try_add_to_dst(src, dst, timestamp, path, "value")
    for key, value in src.items():
        if isinstance(value, dict):
            dst = flatten_json_obj(value, dst, timestamp, path + ([key] if key != "data" else []))

    return dst

def try_add_to_dst(src:dict, dst:dict[Any,dict[str,Any]], timestamp, path:list, key:str):
    if key in src and not isinstance(src[key], dict) and not timestamp is None:
        if "unit" in src and isinstance(src["unit"], str):
            path.append(src["unit"])
        dst[timestamp] = {**dst.get(timestamp, {}), ".".join(path):src[key]}
    return dst


if __name__ == "__main__":
    main()
