import random
import warnings

import pandas as pd
from pandas import DataFrame as DF

from core.config import *
from core.ev_models_info import models_info
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.pandas_utils import map_col_to_dict, set_str_to_lower
from transform.ayvens.ayvens_config import *

def get_fleet_info(bucket=S3_Bucket()) -> DF:
    fleet_info = (
        bucket.read_csv_df(AYVENS_FLEET_INFO_CSV_KEY)
        .rename(columns=str.lower)
        .rename(columns=FLEET_INFO_COLS_NAME_MAPPING)
        .pipe(map_col_to_dict, "version", MODEL_VERSION_NAME_MAPPING)
        .pipe(map_col_to_dict, "make", MAKE_NAME_MAPPING)
        .drop_duplicates(subset="vin")
        .astype(COL_DTYPES)
        .set_index("vin", drop=False)
        .drop(columns=["autonomie", "capacity"])
        .pipe(set_str_to_lower)
        .assign(vin=lambda df: df["vin"].str.upper()) # Dirty workaround to leave VINs in upper case
    )
    fleet_info["model"] = fleet_info["model"].str.lower()
    fleet_info["version"] = fleet_info["version"].str.lower()
    fleet_info["make"] = fleet_info["make"].str.lower()

    # We put this section into a warning catch block as it is meant to be removed
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning, message=".*observed=False.*")
        fleet_info["dummy_soh_maker_offset"] = fleet_info.groupby("make")["vin"].transform(lambda vins: random.uniform(-1, 0.1))
        fleet_info["dummy_soh_model_offset"] = fleet_info.groupby(["make", "version"])["vin"].transform(lambda vins: random.uniform(-1, 0.1))
        fleet_info["dummy_soh_model_slope"] = fleet_info.groupby(["make", "version"])["vin"].transform(lambda vins: random.uniform(SOH_LOST_PER_KM_DUMMY_RATIO - 0.00001, SOH_LOST_PER_KM_DUMMY_RATIO + 0.00001))
        fleet_info["dummy_soh_vehicle_offset"] = fleet_info.groupby(["make", "version"])["vin"].transform(lambda vins: random.uniform(SOH_LOST_PER_KM_DUMMY_RATIO - 0.00001, SOH_LOST_PER_KM_DUMMY_RATIO + 0.00001))

    # Add registration dates from fleet info global NL (NetherLands) 2
    fleet_info_with_registration_and_start_contract = bucket.read_csv_df(AYVENS_FLEET_WITH_CAR_REGISTRATION_KEY).set_index("VIN", drop=False)
    fleet_info["in_GLOBAL2"] = fleet_info["vin"].isin(fleet_info_with_registration_and_start_contract["VIN"])
    vins_in_global2 = fleet_info.query("in_GLOBAL2")["vin"]
    car_registrations = fleet_info_with_registration_and_start_contract.loc[vins_in_global2, "Car registration date"]
    fleet_info.loc[vins_in_global2, "registration_date"] = car_registrations

    # Add contract start dates from fleet info global NL (NetherLands)
    fleet_info_with_start_contract = bucket.read_csv_df(AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE_KEY).set_index("VIN", drop=False)
    fleet_info["in_GLOBAL"] = fleet_info["vin"].isin(fleet_info_with_start_contract["VIN"])
    vins_in_global = fleet_info.query("in_GLOBAL")["vin"]
    contract_start_dates = fleet_info_with_start_contract.loc[vins_in_global, "Contract start datum"]
    fleet_info.loc[vins_in_global, "contract_start_date"] = contract_start_dates
    contract_start_dates = fleet_info_with_registration_and_start_contract.loc[vins_in_global2, "Contract start date"]
    fleet_info.loc[vins_in_global2, "contract_start_date"] = contract_start_dates

    # Add capacity and range of known models stored in ev models info to the fleet info 
    COLS_TO_MERGE_FROM_MODELS_INFO = ["capacity", "range"]
    COLS_TO_MERGE_ON_FROM_MODELS_INFO = ["model", "version"]
    fleet_info = fleet_info.merge(
        models_info[COLS_TO_MERGE_ON_FROM_MODELS_INFO + COLS_TO_MERGE_FROM_MODELS_INFO],
        on=COLS_TO_MERGE_ON_FROM_MODELS_INFO,
        how="left"
    )

    return fleet_info

if __name__ == "__main__":
    single_dataframe_script_main(get_fleet_info)
    
fleet_info = get_fleet_info()
