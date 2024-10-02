"""
This module provides the function `processed_time_series_of` to provide a cleaned/processed time series from Watea's data.
Can also be used as a script to process the entirety of the watea fleet.
"""
from typing import Generator

import numpy as np
from pandas import DataFrame as DF
from pandas import Series
from rich import print
from rich.traceback import install as install_rich_traceback

import core.time_series_processing as ts
from core.caching_utils import instance_data_caching_wrapper
from core.console_utils import parse_kwargs
from analysis.watea.watea_constants import *
from analysis.watea.watea_fleet_info import iterate_over_ids
from analysis.watea.raw_watea_ts import raw_ts_of

def main():
    install_rich_traceback(extra_lines=0, width=130)
    kwargs = parse_kwargs()
    for id, vehicle_df in processed_ts_it(force_update=True):
        print(id)
        print(vehicle_df.columns)

def processed_ts_it(fleet_query: str=None, ts_query=None, force_update:bool=False, **kwargs) -> Generator[tuple[str, DF], None, None]:
    for id in iterate_over_ids(fleet_query, **kwargs):
        ts = processed_ts_of(id, force_update, **kwargs)
        if not ts_query is None:
            ts = ts.query(ts_query)
        yield id, ts

def processed_ts_of(id: str, force_update:bool=False, **kwargs) -> DF:
    return instance_data_caching_wrapper(
        id,
        PATH_TO_PROCESSED_TS.format(id=id),
        lambda vin: process_raw_time_series(raw_ts_of(vin), **kwargs),
        force_update=force_update,
    )

def process_raw_time_series(raw_vehicle_df: DF, **kwargs) -> DF:
    return (
        raw_vehicle_df
        .pipe(pre_process_raw_time_series)
        .pipe(ts.in_motion_mask_from_odo_diff)
        .pipe(ts.in_discharge_and_charge_from_soc_diff)
        .eval("in_charge = in_charge & soc < 99") # Ford E-Transit recordings tend to plateau at 99 of a random amout of time so remove these 
        .eval("power = current * voltage")
        .pipe(ts.perf_mask_and_idx_from_condition_mask, "in_charge", max_time_diff=PERF_MAX_TIME_DIFF)
        .pipe(process_power)
        .pipe(ts.perf_mask_and_idx_from_charge_mask, max_time_diff=PERF_MAX_TIME_DIFF)
        .pipe(ts.perf_mask_and_idx_from_condition_mask, "in_discharge")
        .pipe(ts.perf_mask_and_idx_from_charge_mask, PERF_MAX_TIME_DIFF)
        .pipe(ts.compute_cum_integrals_of_current_vars)
    )

def process_power(vehicle_df: DF) -> DF:
    vehicle_df["window_current_mean"] = (
        vehicle_df["current"]
        .where(vehicle_df["in_charge"], np.nan)
        .mask(vehicle_df["current"].gt(0), np.nan)
        .rolling(TD(minutes=10))
        .mean()
    )
    vehicle_df['power'] = vehicle_df.eval("current * voltage")

    return vehicle_df

def pre_process_raw_time_series(raw_vehicle_df: DF) -> DF:        
    return (
        raw_vehicle_df
        .rename(columns={
            "distance_totalizer": "odometer",
            "battery_hv_soc": "soc",
            "date_translated": "date",
            "battery_hv_temp": "temp",
            "battery_hv_voltage": "voltage",
            "battery_hv_current": "current",
            "autonomy_km": "battery_range_km"
        })
        .pipe(ts.process_date, set_as_index=True, add_sec_time_diff_col=True)
    )


if __name__ == '__main__':
    main()
