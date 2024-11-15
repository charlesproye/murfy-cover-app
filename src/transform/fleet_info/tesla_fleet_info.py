"""
This module provides two dataframes:
- test_tesla_fleet_info: The fleet info of the vehicles from mondayCar and some other vehicles private cars.
- followed_tesla_vehilces_info: A dataframe with one row per vehicle and the columns: vin, model, version.
"""
from logging import getLogger

from core.logging_utils import set_level_of_loggers_with_prefix
from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from transform.fleet_info.config import *

logger = getLogger("transform.fleet_info.tesla_fleet_info")

def get_test_tesla_fleet_info(bucket: S3_Bucket=bucket) -> DF:
    return parse_fleet_info_rep(TEST_TESLA_FLEET_INFO_KEY).assign(owner="test_tesla_fleet")

def get_followed_tesla_vehicles_info(bucket: S3_Bucket=bucket) -> DF:
    fleet_infos = [parse_fleet_info_rep(response_key) for response_key in S3_JSON_FLEET_INFO_RESPONSE_KEYS]
    return (
        pd.concat(fleet_infos)
        .drop_duplicates(subset=["vin"])
    )

def parse_fleet_info_rep(response_key: str) -> DF:    
    response = bucket.read_json_file(response_key)
    filtered_response = (                                               # The response is a list of dicts.
        pd.json_normalize(response, record_path=["codes"], meta=["vin"])# The response is a list of dicts is unstructured json, so we need to normalize it. 
        # After normalizing, we end up with a DF with columns vin, code, displayName, isActive.
        # We pivot the table and end up with a one hot encoded DF of the vehicle's features where each line is a vehicle and each column is a feature.
        .pivot_table(index="vin", columns="displayName", values="isActive", aggfunc=pd.Series.mode, fill_value=False)
        .filter(like="Model", axis=1)                                   # The only columns we are interested in are the model column so we filter out the rest.
    )
    if filtered_response.empty:
        logger.warning(f"No model found in {response_key}")
        return filtered_response
    return(
        filtered_response
        .idxmax(axis=1)                                                 # We take the idxmax of the row, ie:the name of the column, ie: the name of the model, to get the model of the vehicle.                
        .astype("string")                                               # We convert the dtype to string.
        .str                                                            # We use the str accessor to extract the model and version from the string.
        .extract(r'^(?P<model>Model \w+) (?P<version>.+)$')             # We extract the model and version from the string with regex.
        .reset_index(drop=False)
        .pipe(set_all_str_cols_to_lower, but=["vin"])
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.fleet_info.tesla_fleet_info")
    logger.info("=======================test_tesla_fleet_info=======================")
    single_dataframe_script_main(get_test_tesla_fleet_info)
    logger.info("=======================followed_tesla_vehicles_info=======================")
    single_dataframe_script_main(get_followed_tesla_vehicles_info)

test_tesla_fleet_info = get_test_tesla_fleet_info()
followed_tesla_vehicles_info = get_followed_tesla_vehicles_info()
