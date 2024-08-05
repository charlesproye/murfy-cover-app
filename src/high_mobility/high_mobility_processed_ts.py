"""
This script processes raw time series stored in the s3 bucket.
Generates a `processed_ts_meta_data.parquet` file that sums up relevant informations for future processign steps.
Like what vehicule has power data for example.
"""
import logging

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
import numpy as np

from core.time_series_processing import in_discharge_and_charge_from_soc_diff
from core.s3_utils import S3_Bucket
from core.constants import *
from core.console_utils import main_decorator
from high_mobility.high_mobility_constants import *
from core.pandas_utils import print_data

logger = logging.getLogger(__name__)

@main_decorator
def main():
    logging.basicConfig(level=logging.INFO)
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
    processed_ts_metada_data = keys.apply(processed_ts_ETL, bucket=bucket, axis=1)
    # bucket.save_pandas_obj_as_parquet(processed_ts_metada_data, "processed_ts/ts_metadata.parquet")

def processed_ts_ETL(key_data:Series, bucket: S3_Bucket) -> Series:
    """
    ### Description:
    Extracts, processes and loads the raw time series.
    ### Returns:
    Metadata about the time series.
    """
    # print(key_data)
    print(key_data[["brand", "vin"]])
    raw_ts = bucket.read_parquet(key_data["key"])
    processed_ts = process_raw_ts(raw_ts, key_data)
    print("=================")
    dest_key = "/".join(("processed_ts", "time_series", key_data["brand"], key_data["vin"])) + ".parquet"
    bucket.save_pandas_obj_as_parquet(processed_ts, dest_key)

    return compute_metada_data(processed_ts, key_data)

def compute_metada_data(processd_ts: DF, key_data:Series) -> Series:
    return Series({
        "vin": key_data["vin"],
        # "has_power_during_chare": processd_ts
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
    print(ts)
    return (
        ts
        # For some reason, date index seems to get reset when written or read from or to s3...
        .set_index("date", drop=False) 
        .drop_duplicates()
        .rename(columns=COLS_NAME_DICTS)
        .pipe(standardize_units, key_data)
        .pipe(standaridize_soc, "charging_soc")
        .pipe(standaridize_soc, "diagnostics_soc")
        .assign(soc=lambda df:df.get("diagnostic_soc", Series([np.nan] * len(df), dtype="float")))
        .pipe(compute_charging_status_columns)
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

def standaridize_soc(ts:DF, soc_col:str="soc") -> DF:
    """Ensures that the raw soc is a percentage."""
    if soc_col in ts.columns and ts[soc_col].max() <= 1:
        ts[soc_col] *= 100
    return ts

def compute_charging_status_columns(ts: DF) -> DF:
    """
    ### Description:
    Ensure that the time series has a `in_charge` and `in_discharge` column.  
    If the data API provides a charging status it will be used to compute the masks.  
    Otherwise, the soc will be used.  
    """
    # Cases:
    if "charging_status" in ts.columns: 
        ts["in_charge"] = ts["charging_status"].isin(CHARGING_STATUS_TO_CHARGING_VAL)
        ts["in_discharge"] = ts["charging_status"].isin(CHARGING_STATUS_TO_DISCHARGING_VAL)
    else:
        ts = in_discharge_and_charge_from_soc_diff(ts)
    
    return ts
    
if __name__ == "__main__":
    main()
