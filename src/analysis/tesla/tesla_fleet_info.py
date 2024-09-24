"""
This module handles a data frame that holds static data about the vehicles such as model, default capacity and range, ect...
Each line corresponds to a vehicle.
"""
from typing import Generator
import json

import pandas as pd
from pandas import DataFrame as DF
from rich import print
from rich.progress import track

from core.ev_models_info import get_ev_models_infos
from analysis.tesla.tesla_constants import *

def main():
    fleet_info_df = compute_fleet_info()
    print(fleet_info_df.to_string(max_rows=None))
    fleet_info_df.to_csv(INITIAL_CSV_FLEET_INFO_PATH, index=False)
    fleet_info_df.to_parquet(INITIAL_PARQUET_FLEET_INFO_PATH)


def iterate_over_vins(query_str:str=None, use_progress_track=True, **kwarges) -> Generator[str, None, None]:
    filtered_fleet_info_df = fleet_info_df.query(query_str) if query_str else fleet_info_df
    return track(filtered_fleet_info_df["vin"]) if use_progress_track else filtered_fleet_info_df["vin"]
        

def compute_fleet_info() -> DF:
    with open(INITIAL_JSON_FLEET_INFO_PATH) as json_fleet_info_file:
        raw_json_fleet_info: list[dict] = json.load(json_fleet_info_file)
    model_infos = get_ev_models_infos()
    fleet_info_objs: list[dict] = []
    for raw_dict in raw_json_fleet_info:
        vehicle_info_dict = {
            "vin": raw_dict["vin"],
            "model": find_in_list_of_dict(raw_dict, "$MT"),
        }
        vehicle_info_dict["default_kwh_energy_capacity"] = model_infos.at[vehicle_info_dict["model"], "kwh_capacity"]
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

fleet_info_df = pd.read_parquet(INITIAL_PARQUET_FLEET_INFO_PATH)
