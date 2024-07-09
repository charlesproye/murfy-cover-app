from pandas import DataFrame as DF
from rich import print

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
        "motion_perfs": perfs.motion_perfs_df_of(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, cum_energy_spent_col="cum_energy"),
        # "self_discharge_perfs": perfs.self_discharge_df_of(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, cum_energy_spent_col="cum_energy"),
        "discharge_perfs": compute_discharge_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC),
        "charge_perfs": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC),
    }

def compute_discharge_perfs(vehicle_df:DF, default_kwh_per_soc:float) -> DF:
    return (
        perfs.agg_diffs_df_of(
            vehicle_df,
            "in_discharge_perf_mask",
            "in_discharge_perf_idx",
            {"cum_energy": "energy_diff"}
        )
        .pipe(perfs.compute_soh_from_soc_and_energy_diff, "energy_diff", default_kwh_per_soc, "discharge_soh")
    ) 


def compute_charging_perfs(vehicle_df: DF, default_kwh_per_soc:float) -> DF:
    return (
        perfs.agg_diffs_df_of(vehicle_df, "in_charge_perf_mask", "in_charge_perf_idx", {"cum_energy": "energy_added", "battery_range_km": "range_gained"})
        .pipe(perfs.compute_soh_from_soc_and_energy_diff, "energy_added", default_kwh_per_soc, "energy_soh")
        .eval("battery_range_added_soh = 100 * (range_gained / soc_diff) / @FORD_ETRANSIT_DEFAULT_KM_PER_SOC")
        .eval("sec_per_soc = sec_duration / soc_diff")
    )

if __name__ == '__main__':
    main()
