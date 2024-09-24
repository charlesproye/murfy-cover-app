import json

import pandas as pd
from pandas import DataFrame as DF
from rich import print

from core.console_utils import main_decorator
from core.caching_utils import singleton_data_caching
from core.ev_models_info import get_ev_models_infos
from analysis.tesla.tesla_constants import *

@main_decorator
def main():
    fleet_info_df = get_fleet_info(force_update=True)
    print(fleet_info_df.to_string(max_rows=None))

@singleton_data_caching(INITIAL_FLEET_INFO_PATH)
def get_fleet_info() -> DF:
    with open(JSON_FLEET_INFO_RESPONSE_PATH) as json_fleet_info_file:
        raw_json_fleet_info: list[dict] = json.load(json_fleet_info_file)
    models_infos = get_ev_models_infos()
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
    main()

fleet_info_df = pd.read_parquet(INITIAL_FLEET_INFO_PATH)
