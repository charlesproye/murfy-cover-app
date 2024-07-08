"""
This module provides the function `processed_time_series_of` to provide a cleaned/processed time series from Tesla's personal API.
Can also be used as a script to process the entirety of the tesla data.
"""
from typing import Generator

from pandas import Series
from pandas import DataFrame as DF
from rich import print

import core.time_series_processing as ts
from tesla_constants import *
from core.caching_utils import data_caching_wrapper
from raw_tesla_ts import raw_ts_of
from tesla_fleet_info import iterate_over_vins, fleet_info_df
from core.argparse_utils import parse_kwargs

def main():
    kwargs = parse_kwargs()
    for vin, vehicle_df in iterate_overs_processed_ts(force_update=True, **kwargs):
        print(vehicle_df)

def iterate_overs_processed_ts(**kwargs) -> Generator[tuple[str, DF], None, None]:
    for vin in iterate_over_vins(**kwargs):
        vehicle_df = processed_time_series_of(vin, **kwargs)

        yield vin, vehicle_df

def processed_time_series_of(vin:str, force_update:bool=False, **kwargs) -> DF:
    return data_caching_wrapper(
        vin,
        PATH_TO_PROCESSED_TESLA_TS,
        lambda vin: process_raw_time_series(raw_ts_of(vin), vin, **kwargs),
        force_update=force_update,
    )

def process_raw_time_series(raw_vehicle_df: DF, vin:str, **kwargs) -> DF:
    vehicle_df = preprocess_raw_time_series(raw_vehicle_df)
    vehicle_df = vehicle_df.assign(
        cum_energy_spent=ts.cum_energy_from_power(vehicle_df["power"]),
        cum_charging_energy=ts.cum_energy_from_power(vehicle_df["charger_power"]),
    )
    vehicle_df = ts.soh_from_est_battery_range(vehicle_df, "battery_range_km", MODEL_Y_REAR_DRIVE_MIN_KM_PER_SOC)
    vehicle_df = ts.in_motion_mask_from_odo_diff(vehicle_df)
    vehicle_df = in_charge_perf_mask(vehicle_df)
    vehicle_df = ts.self_discharge(vehicle_df)
    vehicle_df = last_charge_soh(vehicle_df, vin)

    return vehicle_df

def preprocess_raw_time_series(raw_vehicle_df: DF) -> DF:
    return (
        raw_vehicle_df
        # Duplicate columns to keep previous value for comparaison during plotting/debugging
        # Standardize distance units to km
        .assign(
            soc=raw_vehicle_df["battery_level"],
            battery_range_km=MILE_TO_KM * raw_vehicle_df["battery_range"],
            charge_km_added=MILE_TO_KM * raw_vehicle_df["charge_miles_added_ideal"]
        )
        # Standardize names to be used in core/common functions used for all data providers
        .rename(columns={
            "readable_date": "date",
            "battery_level": "raw_soc",
        })
        .set_index("date", drop=False)
        .drop_duplicates("date")
        .sort_index()
    )

def last_charge_soh(vehicle_df: DF, vin:str) -> DF:
    vehicle_df["energy_per_range_km"] = vehicle_df["charge_energy_added"].rolling(window="1h", center=True, min_periods=10).mean() / vehicle_df["charge_km_added"].rolling(window="1h", center=True, min_periods=10).mean() #vehicle_df.eval("charge_energy_added / charge_km_added")
    vehicle_df["energy"] = vehicle_df.eval("battery_range_km * energy_per_range_km")
    vehicle_df["interpolated_energy"] = vehicle_df["energy"].mask(vehicle_df["energy"].diff().eq(0, fill_value=False), np.nan).interpolate(method="time")
    vehicle_df["interpolated_soc"] = vehicle_df["soc"].mask(vehicle_df["soc"].diff().eq(0, fill_value=False), np.nan).interpolate(method="time")
    vehicle_df["last_charge_soh"] = 100 * (vehicle_df["interpolated_energy"] / vehicle_df["interpolated_soc"]) / fleet_info_df.at[vin, "default_kwh_per_soc"]

    return vehicle_df

def in_charge_perf_mask(vehicle_df: DF) -> DF:
    vehicle_df["in_charge"] = vehicle_df.eval("charging_state == 'Charging'")
    vehicle_df["in_charge_idx"] = ts.period_idx_of_mask(vehicle_df["in_charge"])
    vehicle_df["in_charge_perf_mask"] = vehicle_df.groupby("in_charge_idx")["soc"].transform(ts.trim_off_mask_perf_period) & vehicle_df["in_charge"]
    vehicle_df["in_charge_perf_idx"] = ts.period_idx_of_mask(vehicle_df["in_charge_perf_mask"])

    return vehicle_df

if __name__ == "__main__":
    main()
