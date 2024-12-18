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
    return (
        parse_fleet_info_rep(TEST_TESLA_FLEET_INFO_KEY)
        .assign(
            owner="test_tesla_fleet",
            make="Tesla",
        )
    )

def get_followed_tesla_vehicles_info(bucket: S3_Bucket=bucket) -> DF:
    fleet_infos = [parse_fleet_info_rep(response_key, bucket) for response_key in S3_JSON_FLEET_INFO_RESPONSE_KEYS]
    return (
        pd.concat(fleet_infos)
        .drop_duplicates(subset=["vin"])
    )

def parse_fleet_info_rep(response_key: str, bucket: S3_Bucket=bucket) -> DF:    
    response = bucket.read_json_file(response_key)
    # The response is a list of dicts.
    fleet_info = pd.json_normalize(response, record_path=["codes"], meta=["vin"])# The response is a list of dicts is unstructured json, so we need to normalize it. 
    # After normalizing, we end up with a DF with columns vin, code, displayName, isActive.
    # We pivot the table and end up with a one hot encoded DF of the vehicle's features where each line is a vehicle and each column is a feature.
    fleet_info:DF = (
        fleet_info
        # The only columns we are interested in are the model column so we filter out the rest.
        .query('code.str.startswith("$MT")', engine='python')
        .eval("model = code.str[3]")
    )
    fleet_info['model'] = fleet_info['model'].map({"1": "S", "7": "S"}).fillna(fleet_info["model"])
    fleet_info['model'] = "model " + fleet_info['model']
    fleet_info = fleet_info.rename(columns={"displayName": "version"})
    fleet_info['version'] = fleet_info['version'].str.replace(r'^Model \w+ (.+)$', r'\1', regex=True)
    fleet_info = set_all_str_cols_to_lower(fleet_info.astype({"model": "string", "version": "string"}), but=["vin"])
    fleet_info = fleet_info.set_index("vin", drop=False)
    fleet_info = fleet_info.drop(columns=["colorCode", "isActive"])
    fleet_info = fleet_info.rename(columns={"code": "tesla_code"})

    return fleet_info

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.fleet_info.tesla_fleet_info")
    logger.info("=======================test_tesla_fleet_info=======================")
    #single_dataframe_script_main(get_test_tesla_fleet_info)
    logger.info("=======================followed_tesla_vehicles_info=======================")
    single_dataframe_script_main(get_followed_tesla_vehicles_info)

test_tesla_fleet_info = get_test_tesla_fleet_info()
followed_tesla_vehicles_info = get_followed_tesla_vehicles_info()
