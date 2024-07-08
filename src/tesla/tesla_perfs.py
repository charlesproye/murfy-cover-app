"""
This module implements aggregation operation over periods of the processed time series.
e.g: charging period, trip periods
As a script, this computes the entirety of the periods of the fleet.
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
from core.perf_agg_processing import compute_soh_from_soc_and_energy_diff, agg_diffs_df_of
from tesla_constants import *
import  core.perf_agg_processing as perfs
from processed_tesla_ts import iterate_overs_processed_ts, processed_time_series_of
from core.plt_utils import plt_single_vehicle_sohs
from tesla_fleet_info import fleet_info_df

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--vin', type=str, help='Optional VIN string')
    parser.add_argument('--y_col', type=str, default='odometer', help='Optional y_col string (default: "odometer")')
    parser.add_argument('--y_col_perf', type=str, default='mean_odo', help='Optional y_col_perf string (default: "mean_odo")')

    args = parser.parse_args()
    vin = args.vin

    if vin:
        vehicle_df = processed_time_series_of(vin)
        perfs = compute_all_perfs(vehicle_df, vin)
        plt_single_vehicle_sohs(vehicle_df, perfs, x_col=args.y_col, y_col_periods=args.y_col_perf, plt_variance=True)

def iterate_over_perfs_df(**kwargs) -> Generator[tuple[str, dict[str, DF]], None, None]:
    for vin, vehicle_df in iterate_overs_processed_ts(**kwargs):
        yield vin, compute_all_perfs(vehicle_df, vin)


def compute_all_perfs(vehicle_df: DF, vin:str) -> dict[str, DF]:
    default_kwh_per_soc = fleet_info_df.at[vin, "default_kwh_per_soc"]

    return {
        "motion_perfs": perfs.motion_perfs_df_of(vehicle_df, default_kwh_per_soc),
        "self_discharge_perfs": perfs.self_discharge_df_of(vehicle_df, default_kwh_per_soc),
        "charging_perfs": compute_charging_perfs(vehicle_df, default_kwh_per_soc),
    }


def compute_charging_perfs(vehicle_df: DF, default_kwh_per_soc:float) -> DF:
    charging_perfs_df = agg_diffs_df_of(
        vehicle_df,
        "in_charge_perf_mask",
        "in_charge_idx",
        {
            "cum_charging_energy": "charger_cum_energy",
            "charge_energy_added": "energy_added_sum",
            "cum_energy_spent": "energy_spent",
            "charge_miles_added_ideal": "range_gained",
        }
    )
    charging_perfs_df = compute_soh_from_soc_and_energy_diff(charging_perfs_df, "charger_cum_energy", default_kwh_per_soc, "soh_cum_charger_energy")
    charging_perfs_df = compute_soh_from_soc_and_energy_diff(charging_perfs_df, "energy_added_sum", default_kwh_per_soc, "energy_soh")
    charging_perfs_df["battery_range_added_soh"] = 100 * MILE_TO_KM * charging_perfs_df.eval("range_gained / soc_diff") / MODEL_Y_REAR_DRIVE_MIN_KM_PER_SOC
    charging_perfs_df["sec_per_soc"] = charging_perfs_df["duration"].dt.total_seconds() / charging_perfs_df["soc_diff"]
    charging_perfs_df["mean_odo"] = charging_perfs_df.eval("start_odometer + distance / 2")

    return charging_perfs_df



if __name__ == "__main__":
    main()
