"""
This script output all the value needed for the tesla passport 
"""
from glob import glob
from os import path
import argparse
from typing import Generator

import pandas as pd
from pandas import DataFrame as DF
from rich import print
from rich.progress import track

import core.time_series_processing as ts
import core.perf_agg_processing as perfs
from core.perf_agg_processing import compute_soh_from_soc_and_energy_diff, agg_diffs_df_of
from tesla.tesla_constants import *
from tesla.tesla_fleet_info import fleet_info_df
from tesla.processed_tesla_ts import iterate_over_processed_ts, processed_ts_of

if __name__ == "__main__":
    main()

def main():
    get_odometer_ts()
    get_soh
    get_initial_autonomy_ts()
    get_energy_diff_ts()
    get_expected_range_ts()
    
    


def iterate_over_vins(query_str:str=None, use_progress_track=True, **kwarges    ) -> Generator[str, None, None]:
    filtered_fleet_info_df = fleet_info_df.query(query_str) if query_str else fleet_info_df
    return track(filtered_fleet_info_df["vin"]) if use_progress_track else filtered_fleet_info_df["vin"]
        

def compute_fleet_info() -> DF:
    with open(path.join(PATH_TO_FLEET_INFO_FOLDER, "raw_fleet_info.json")) as json_fleet_info_file:
        raw_json_fleet_info: list[dict] = json.load(json_fleet_info_file)
    model_infos = (
        pd.read_csv(PATH_TO_MODELS_INFO)
        .astype({
            "model": "string",
            "manufacturer": "string",
            "kwh_capacity": "float",
        })
        .set_index("model")
    )
    fleet_info_objs: list[dict] = []
    for raw_dict in raw_json_fleet_info:
        vehicle_info_dict = {
            "vin": raw_dict["vin"],
            "model": find_in_list_of_dict(raw_dict, "$MT"),
        }
        vehicle_info_dict["default_kwh_energy_capacity"] = model_infos.at[vehicle_info_dict["model"], "kwh_capacity"]
        vehicle_info_dict["default_kwh_per_soc"] = vehicle_info_dict["default_kwh_energy_capacity"] / 100
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

fleet_info_df = pd.read_parquet(path.join(PATH_TO_FLEET_INFO_FOLDER, "fleet_info_df.paruet"))
