from pandas import DataFrame as DF
import numpy as np

from watea.watea_constants import *
from core.argparse_utils import parse_kwargs
from watea.processed_watea_ts import iterate_over_processed_ts
import core.perf_agg_processing as perfs

def main():
    kwargs = parse_kwargs()
    for id, vehicle_df in iterate_over_processed_ts(force_update=True, **kwargs):
        perfs_dict = compute_perfs(vehicle_df)

def compute_perfs(vehicle_df: DF) -> dict[str, DF]:
    return {
        "discharge": compute_discharge_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC),
        "charge_above_80": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge_above_80", "energy_soh_above_80"),
        "charge_bellow_80": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge_bellow_80", "energy_soh_bellow_80"),
    }

def compute_discharge_perfs(vehicle_df:DF, default_kwh_per_soc:float) -> DF:
    return (
        vehicle_df
        .pipe(perfs.agg_diffs_df_of, {"cum_energy": "energy_diff"}, "in_discharge",)
        .pipe(perfs.compute_soh_from_soc_and_energy_diff, "energy_diff", default_kwh_per_soc, "discharge_soh")
        .pipe(lambda df: df.assign(discharge_soh=df["discharge_soh"].replace(0, np.nan)))
        .eval("km_per_soc = distance / soc_diff")
        .pipe(lambda df: df.assign(km_per_soc=df["km_per_soc"].replace(0, np.nan)))
    ) 

def compute_charging_perfs(vehicle_df: DF, default_kwh_per_soc:float, in_charge_mask:str, energy_soh_name:str) -> DF:
    return (
        vehicle_df
        .pipe(perfs.agg_diffs_df_of, {"cum_energy": "energy_added", "battery_range_km": "range_gained"}, in_charge_mask)
        .pipe(perfs.compute_soh_from_soc_and_energy_diff, "energy_added", default_kwh_per_soc, energy_soh_name)
        .pipe(lambda df: df.assign(energy_soh=df[energy_soh_name].replace(0, np.nan)))
        .eval("battery_range_added_soh = 100 * (range_gained / soc_diff) / @FORD_ETRANSIT_DEFAULT_KM_PER_SOC")
        .eval("sec_per_soc = sec_duration / soc_diff")
    )

if __name__ == '__main__':
    main()
