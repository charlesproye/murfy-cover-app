from dateutil import parser
from logging import Logger, getLogger
import random

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.constants import *
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import singleton_data_caching
from core.pandas_utils import concat, map_col_to_dict
from transform.ayvens.config import *

@singleton_data_caching(AYVENS_FLEET_INFO_PARQUET)
def get_fleet_info() -> DF:
    fleet_info = (
        pd.read_csv(AYVENS_FLEET_INFO_CSV)
        .rename(columns=str.lower)
        .pipe(map_col_to_dict, "type", MODEL_VERSION_NAME_MAPPING)
        .pipe(map_col_to_dict, "make", MAKE_NAME_MAPPING)

    )
    fleet_info["maker_offset"] = fleet_info.groupby("make")["vin"].transform(lambda vins: random.uniform(-1, 0.1))
    fleet_info["model_offset"] = fleet_info.groupby(["make", "type"])["vin"].transform(lambda vins: random.uniform(-1, 0.1))
    fleet_info["model_slope"] = fleet_info.groupby(["make", "type"])["vin"].transform(lambda vins: random.uniform(SOH_LOST_PER_KM_DUMMY_RATIO - 0.00001, SOH_LOST_PER_KM_DUMMY_RATIO + 0.00001))

    return fleet_info

if __name__ == "__main__":
    single_dataframe_script_main(get_fleet_info, force_update=True)
    
fleet_info = get_fleet_info()
