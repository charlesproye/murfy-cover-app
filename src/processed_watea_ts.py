"""
This module provides the function `processed_time_series_of` to provide a cleaned/processed time series from Watea's data.
Can also be used as a script to process the entirety of the watea fleet.
"""
from typing import Generator

from pandas import DataFrame as DF
from rich import print

import core.time_series_processing as ts
from watea_constants import *
from core.caching_utils import data_caching_wrapper
from watea_fleet_info import iterate_over_ids
from core.argparse_utils import parse_kwargs
from raw_watea_ts import raw_ts_of
from core.plt_utils import plt_time_series

def main():
    kwargs = parse_kwargs()
    for id, vehicle_df in iterate_over_processed_ts(force_update=True, **kwargs):
        print(id)
        print(vehicle_df)
        if kwargs.get("plt_sohs", False):
            plt_time_series(vehicle_df, )

def iterate_over_processed_ts(query_str: str=None, force_update:bool=False, **kwargs) -> Generator[tuple[str, DF], None, None]:
    for id in iterate_over_ids(query_str, **kwargs):
        yield id, processed_ts_of(id, force_update, **kwargs)

def processed_ts_of(id: str, force_update:bool=False, **kwargs) -> DF:
    return data_caching_wrapper(
        id,
        PATH_TO_PROCESSED_TS.format(id=id),
        lambda vin: process_raw_time_series(raw_ts_of(vin), vin, **kwargs),
        force_update=force_update,
    )

def process_raw_time_series(raw_vehicle_df: DF, id:str) -> DF:
    return (
        raw_vehicle_df
        .rename(columns={
            "distance_totalizer": "odometer",
            "battery_hv_soc": "soc",
            "date_translated": "date",
            "battery_hv_temp": "temp",
            "battery_hv_voltage": "voltage",
            "battery_hv_current": "current",
        })
        .drop_duplicates("date")
        .set_index("date", drop=False)
        .sort_index()
        .pipe(ts.soh_from_est_battery_range, "autonomy_km", FORD_ETRANSIT_DEFAULT_KM_PER_SOC)
        .pipe(ts.in_motion_mask_from_odo_diff)
        .pipe(ts.in_charge_and_discharge_mask_fromo_soc_diff)
        .eval("power = current * voltage")
        .pipe(ts.add_cum_energy_from_power_cols, "power", "energy")
    )

if __name__ == '__main__':
    main()
