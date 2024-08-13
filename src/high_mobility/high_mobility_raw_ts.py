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
    # keys = Series(bucket.list_keys("response"))
    keys = pd.read_csv("response_keys.csv")["0"].astype("string")
    # print(keys.to_string(max_rows=None, ))
    if len(keys) == 0:
        logger.info("""
            No responses found in the 'response' folder.
            No raw time series have been generated.
        """)
        return
    # Only retain .cbor responses
    keys = keys[keys.str.endswith(".json")]
    # Reponses are organized as follow response/brand_name/vin/response_timestamp.cbor
    # We extract the brand and vin
    keys = pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns").iloc[:, 0:-1]
    keys.columns = ["key", "brand", "vin", "file"] # Set column names
    keys['is_valid_file'] = (
        keys['file']
        .str.split(".", expand=True).loc[:, 0]
        .str.match(r'^\d{4}-\d{2}-\d{2}$')
    )
    keys = keys.query("is_valid_file")
    # for each group of responses for a vin create a raw time series parquet
    keys.groupby(["brand", "vin"]).apply(parse_responses_as_raw_ts, bucket, include_groups=False)

def parse_responses_as_raw_ts(src_keys:DF, bucket:S3_Bucket):
    raw_jsons:Series = src_keys["key"].apply(bucket.read_json_file)                          # Read responses
    parsed_dfs = []
    for raw_json in raw_jsons:
        try:
            parsed_dfs.append(parse_response_as_df(raw_json))
        except Exception as e:
            logger.warning(f"Caught exception wile parsing response of {'/'.join(src_keys.name)}:\n{e}")
    if len(parsed_dfs) == 0:
        logger.warning(f"No data could be parsed from keys {src_keys['key'].values} for vin {src_keys.name[1]}")
        return
    raw_df:DF = pd.concat(parsed_dfs)   # Parse and concat them into a single df 
    dest_key = "/".join(["raw_ts", "time_series", *src_keys.name]) + ".parquet"         # Create path to save the raw ts. Note: src_keys.name will be defined by the grouby's by argument (see doc)
    bucket.save_pandas_obj_as_parquet(raw_df, dest_key)                                 # save the raw ts


if __name__ == "__main__":
    main()
