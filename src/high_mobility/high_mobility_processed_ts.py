"""
This script processes raw time series stored in the s3 bucket.
Generates a `processed_ts_meta_data.parquet` file that sums up relevant informations for future processign steps.
Like what vehicule has power data for example.
"""
import logging

from rich import print
from rich.traceback import install as install_rich_traceback
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF

from core.s3_utils import S3_Bucket
from core.constants import *
from high_mobility.high_mobility_constants import *
from core.pandas_utils import print_data

logger = logging.getLogger(__name__)

def main():
    install_rich_traceback(extra_lines=0, width=130)
    bucket = S3_Bucket()
    # Get list of objects in response key
    keys = Series(bucket.list_keys("raw_ts/time_series"))
    # Reponses are organized as follow response/brand_name/vin/response_timestamp.cbor
    # We extract the brand and vin according to that pattern
    keys = pd.concat((keys, keys.str.split("/", expand=True).loc[:, 2:]), axis="columns") 
    keys.columns = ["key", "brand", "vin"] # Set column names
    keys["vin"] = keys["vin"].str.strip(".parquet")
    # for each group of responses for a vin create a raw time series parquet
    # print(keys)
    processed_ts_metada_data = keys.apply(raw_ts_ETL, bucket=bucket, axis=1)
    bucket(processed_ts_metada_data, "processed_ts/metadata.parquet")

def raw_ts_ETL(key_data:Series, bucket: S3_Bucket) -> Series:
    """
    ### Description:
    Extracts, processes and loads the raw time series.
    ### Returns:
    Metadata about the time series.
    """
    # print(key_data)
    raw_ts = bucket.read_parquet(key_data["key"])
    processed_ts = process_raw_ts(raw_ts, key_data)
    print("=================")
    dest_key = "/".join(("processed_ts", "time_series", key_data["brand"], key_data["vin"])) + ".parquet"
    # bucket.save_pandas_obj_as_parquet(processed_ts, dest_key)

    return compute_metada_data(processed_ts)

def compute_metada_data(processd_ts: DF, key_data:Series) -> Series:
    return Series({
        "vin": key_data["vin"],
        "has_power_during_chare": ""
    })

def process_raw_ts(ts:DF, key_data:Series) -> DF:
    """
    ### Description:
    Applies the following processing steps to the raw ts:
    - unit ocnversion
    - column name standardization
    - charing status imputing
    ### Returns:
    Dataframe with the (ideally) the following columns:
    - soc
    - odometer
    - in_charge
    - in_discharge  
    *Do not merge the last two into one column as we sometime don't know if the vehicule is charging or discharging.*
    """
    return (
        ts
        .drop_duplicates()
        .pipe(standardize_units, key_data)
        .pipe(print_data)
        .rename(columns=COLS_NAME_DICTS)
        .pipe(print_data)
    )

def standardize_units(ts: DF, key_data:Series) -> DF:
    standardized_unit_cols = []
    col:str
    for col in ts.columns:
        if not col:
            logger.warn(f"Found empty col in raw_ts of {key_data['vin']}, {key_data['brand']}")
            continue
        col_path = col.split(".")
        col_path_end = col_path[-1]
        if col_path_end in UNIT_CONVERSION_OPS:
            ts[col] = UNIT_CONVERSION_OPS[col_path_end](ts[col])
            standardized_unit_cols.append(".".join(col_path[:-1]))
        else:
            standardized_unit_cols.append(col)
    ts.columns = standardized_unit_cols

    return ts

def compute_charging_status_columns(ts: DF) -> DF:
    # Cases:
    if "charging_status" in ts.columns: # 
        

if __name__ == "__main__":
    main()
