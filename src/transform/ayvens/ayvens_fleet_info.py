import random

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.constants import *
from core.console_utils import single_dataframe_script_main
from core.caching_utils import singleton_data_caching
from core.pandas_utils import map_col_to_dict
from transform.ayvens.ayvens_config import *

@singleton_data_caching(AYVENS_FLEET_INFO_PARQUET)
def get_fleet_info() -> DF:
    fleet_info = (
        pd.read_csv(AYVENS_FLEET_INFO_CSV)
        .rename(columns=str.lower)
        .rename(columns=FLEET_INFO_COLS_NAME_MAPPING)
        .pipe(map_col_to_dict, "version", MODEL_VERSION_NAME_MAPPING)
        .pipe(map_col_to_dict, "make", MAKE_NAME_MAPPING)
        .drop_duplicates(subset="vin")
        .set_index("vin", drop=False)
    )
    fleet_info["dummy_soh_maker_offset"] = fleet_info.groupby("make")["vin"].transform(lambda vins: random.uniform(-1, 0.1))
    fleet_info["dummy_soh_model_offset"] = fleet_info.groupby(["make", "version"])["vin"].transform(lambda vins: random.uniform(-1, 0.1))
    fleet_info["dummy_soh_model_slope"] = fleet_info.groupby(["make", "version"])["vin"].transform(lambda vins: random.uniform(SOH_LOST_PER_KM_DUMMY_RATIO - 0.00001, SOH_LOST_PER_KM_DUMMY_RATIO + 0.00001))
    fleet_info["dummy_soh_vehicle_offset"] = fleet_info.groupby(["make", "version"])["vin"].transform(lambda vins: random.uniform(SOH_LOST_PER_KM_DUMMY_RATIO - 0.00001, SOH_LOST_PER_KM_DUMMY_RATIO + 0.00001))

    # Add registration dates from fleet info global NL (NetherLands) 2
    fleet_info_with_registration_and_start_contract = pd.read_csv(AYVENS_FLEET_WITH_CAR_REGISTRATION).set_index("VIN", drop=False)
    fleet_info["in_GLOBAL2"] = fleet_info["vin"].isin(fleet_info_with_registration_and_start_contract["VIN"])
    vins_in_global2 = fleet_info.query("in_GLOBAL2")["vin"]
    car_registrations = fleet_info_with_registration_and_start_contract.loc[vins_in_global2, "Car registration date"]
    fleet_info.loc[vins_in_global2, "registration date"] = car_registrations

    # Add contract start dates from fleet info global NL (NetherLands)
    fleet_info_with_start_contract = pd.read_csv(AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE).set_index("VIN", drop=False)
    fleet_info["in_GLOBAL"] = fleet_info["vin"].isin(fleet_info_with_start_contract["VIN"])
    vins_in_global = fleet_info.query("in_GLOBAL")["vin"]
    contract_start_dates = fleet_info_with_start_contract.loc[vins_in_global, "Contract start datum"]
    fleet_info.loc[vins_in_global, "contract_start_date"] = contract_start_dates
    contract_start_dates = fleet_info_with_registration_and_start_contract.loc[vins_in_global2, "Contract start date"]
    fleet_info.loc[vins_in_global2, "contract_start_date"] = contract_start_dates

    return fleet_info

if __name__ == "__main__":
    single_dataframe_script_main(get_fleet_info, force_update=True)
    
fleet_info = get_fleet_info()
