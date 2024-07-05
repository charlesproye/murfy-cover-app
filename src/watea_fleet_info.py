"""
This module handles a data frame that holds static data about the vehicles such as model, default capacity and range, ect...
Each line corresponds to a vehicle.
"""
from glob import glob
from os import path
from typing import Generator

import pandas as pd
from pandas import DataFrame as DF
from rich import print
from rich.progress import track

from core.caching_utils import safe_cache_to
from watea_constants import *

def main():
    fleet_info_df = compute_fleet_info()
    print(fleet_info_df)
    safe_cache_to(fleet_info_df, PATH_TO_FLEET_INFO_DF.format(extension="csv"), index=False)
    safe_cache_to(fleet_info_df, PATH_TO_FLEET_INFO_DF.format(extension="parquet"))


def iterate_over_ids(query_str:str=None, use_progress_track=True, **kwarges) -> Generator[str, None, None]:
    filtered_fleet_info_df = fleet_info_df.query(query_str) if query_str else fleet_info_df
    return track(filtered_fleet_info_df["id"]) if use_progress_track else filtered_fleet_info_df["id"]
        

def compute_fleet_info() -> DF:
    from processed_watea_ts import process_raw_time_series

    fleet_info_dicts: list[dict] = []
    for file in track(glob(path.join(PATH_TO_RAW_TS_FOLDER, "*.snappy.parquet"))):
        raw_ts = pd.read_parquet(file)
        vehicle_df = process_raw_time_series(raw_ts)
        fleet_info_dicts.append({
            "id": file.split('.')[0].split("=")[1],
            "len": len(vehicle_df),
            **(100 * vehicle_df.count() / len(vehicle_df)).add_suffix("_not_nan_percentage").to_dict(),
            "min_odo": vehicle_df["odometer"].min(),
            "max_odo": vehicle_df["odometer"].max(),
        })
    fleet_info_df = (
        DF(fleet_info_dicts)
        .set_index("id", drop=False)
        .eval("has_elec_records = voltage_not_nan_percentage != 0 & current_not_nan_percentage != 0")
        .eval("has_temp_records = temp_not_nan_percentage != 0")
    )

    return fleet_info_df

if __name__ == "__main__":
    main()

fleet_info_df: DF
if path.exists(PATH_TO_FLEET_INFO_DF.format(extension="parquet")):
    fleet_info_df = pd.read_parquet(PATH_TO_FLEET_INFO_DF.format(extension="parquet"))
else:
    print("[yellow]Warning:", "No fleet info df cached at", f"[blue]{PATH_TO_FLEET_INFO_DF.format(extension='parquet')}")

