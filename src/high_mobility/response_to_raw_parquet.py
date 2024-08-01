"""
This script converts High Mobility json responses into raw parquet time series.
The script operates on S3 buckets. 
"""
from rich import print
from rich.traceback import install as install_rich_traceback
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF

from high_mobility.parse_HighMobility_response import parse_json_as_df
from core.s3_utils import S3_Bucket

def main():
    install_rich_traceback(extra_lines=0, width=130)
    bucket = S3_Bucket()
    
    keys = Series(bucket.list_keys("response"))
    keys = keys[keys.str.endswith(".cbor")]
    keys = pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns")
    keys.columns = ["key", "brand", "vin", "file"]
    print(keys)
    keys.groupby(["brand", "vin"]).apply(parse_responses_as_raw_ts, bucket, include_groups=False)

def parse_responses_as_raw_ts(src_keys:DF, bucket:S3_Bucket):
    raw_jsons:Series = src_keys["key"].apply(bucket.read_cbor)
    raw_df:DF = pd.concat([parse_json_as_df(raw_json) for raw_json in raw_jsons])
    raw_df = raw_df[~raw_df.duplicated()].sort_index()
    dest_key = "/".join(["parquet", *src_keys.name]) + ".parquet"
    bucket.save_pandas_obj_as_parquet(raw_df, dest_key)


if __name__ == "__main__":
    main()
