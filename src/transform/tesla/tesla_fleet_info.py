import json

import pandas as pd
from pandas import DataFrame as DF

from core.console_utils import single_dataframe_script_main
from core.s3_utils import S3_Bucket
from core.caching_utils import cache_result_in_s3
from core.ev_models_info import get_ev_models_infos
from transform.tesla.tesla_config import *

@cache_result_in_s3(S3_INITIAL_FLEET_INFO_KEY)
def get_fleet_info(bucket: S3_Bucket=S3_Bucket()) -> DF:
    raw_json_fleet_info: list[dict] = bucket.read_json_file(S3_JSON_FLEET_INFO_RESPONSE_KEY)
    models_infos = get_ev_models_infos().set_index('model')
    print(models_infos)
    fleet_info_objs: list[dict] = []
    for raw_dict in raw_json_fleet_info:
        vehicle_info_dict = {
            "vin": raw_dict["vin"],
            "model": find_in_list_of_dict(raw_dict, "$MT"),
        }
        vehicle_info_dict["default_kwh_energy_capacity"] = models_infos.at[vehicle_info_dict["model"], "kwh_capacity"]
        fleet_info_objs.append(vehicle_info_dict)

    fleet_info_df = DF.from_dict(fleet_info_objs).set_index("vin", drop=False)

    return fleet_info_df

def find_in_list_of_dict(raw_vehicle_info_dict: dict, target_key_perfix):
    for dictionary in raw_vehicle_info_dict["codes"]:
        if dictionary["code"].startswith(target_key_perfix):
            return dictionary["displayName"]
    raise ValueError(f"Could not find object wth key prefix '{target_key_perfix}' in list of dictonnary.")    

if __name__ == "__main__":
    single_dataframe_script_main(get_fleet_info, force_update=True)

fleet_info_df = get_fleet_info()
