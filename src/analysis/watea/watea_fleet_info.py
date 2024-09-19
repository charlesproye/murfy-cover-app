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

from core.caching_utils import save_cache_to
from watea.watea_constants import *

def main():
    fleet_info_df = compute_fleet_info()
    print(fleet_info_df)
    save_cache_to(fleet_info_df, PATH_TO_FLEET_INFO_DF.format(extension="csv"), index=False)
    save_cache_to(fleet_info_df, PATH_TO_FLEET_INFO_DF.format(extension="parquet"))


def iterate_over_ids(query_str:str=None, use_progress_track=True, track_kwargs={}) -> Generator[str, None, None]:
    filtered_fleet_info_df = fleet_info_df.query(query_str) if not query_str is None else fleet_info_df
    return track(filtered_fleet_info_df["id"], **track_kwargs) if use_progress_track else filtered_fleet_info_df["id"]
        

def compute_fleet_info() -> DF:
    from watea.processed_watea_ts import process_raw_time_series

    fleet_info_dicts: list[dict] = []
    for file in track(glob(path.join(PATH_TO_RAW_TS_FOLDER, "*.snappy.parquet"))):
        raw_ts = pd.read_parquet(file)
        vehicle_df = process_raw_time_series(raw_ts)
        discharge_grps = vehicle_df.query("in_discharge_perf_mask").groupby("in_discharge_perf_idx")
        charge_grps = vehicle_df.query("in_charge_perf_mask").groupby("in_charge_perf_idx")
        fleet_info_dicts.append({
            "id": path.basename(file.split('.')[0]),
            "len": len(vehicle_df),
            **(100 * vehicle_df[["power", "temp"]].count() / len(vehicle_df)).add_suffix("_not_nan_percentage").to_dict(),
            "min_odo": vehicle_df["odometer"].min(),
            "max_odo": vehicle_df["odometer"].max(),
            "power_during_discharge_pct": (100 * discharge_grps["power"].count() / discharge_grps.size()).mean(),
            "power_during_charge_pct": (100 * charge_grps["power"].count() / charge_grps.size()).mean(),
        })
        print(fleet_info_dicts[-1]["power_during_discharge_pct"])
        print(fleet_info_dicts[-1]["power_during_charge_pct"])
    fleet_info_df = (
        DF(fleet_info_dicts)
        .set_index("id", drop=False)
        .eval("has_power_during_discharge = power_during_discharge_pct > 90")
        .eval("has_power_during_charge = power_during_charge_pct > 90")
    )

    return fleet_info_df

if __name__ == "__main__":
    main()

fleet_info_df: DF
if path.exists(PATH_TO_FLEET_INFO_DF.format(extension="parquet")):
    fleet_info_df = pd.read_parquet(PATH_TO_FLEET_INFO_DF.format(extension="parquet"))
else:
    print("[yellow]Warning:", "No fleet info df cached at", f"[blue]{PATH_TO_FLEET_INFO_DF.format(extension='parquet')}")

