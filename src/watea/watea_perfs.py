import pandas as pd
from pandas import DataFrame as DF
import numpy as np
from rich import print
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.dates import date2num
from matplotlib.figure import Figure
from rich.traceback import install as install_rich_traceback

from watea.fleet_watea_wise_perfs import default_100_soh_charge_energy_dist, compute_charge_energy_dist
from watea.watea_constants import *
from watea.processed_watea_ts import iterate_over_processed_ts
from core.argparse_utils import parse_kwargs
import core.perf_agg_processing as perfs
from core.caching_utils import data_caching_wrapper
from core.pandas_utils import *

def main():
    install_rich_traceback(extra_lines=0, width=130)
    kwargs = parse_kwargs(optional_args={"task":"compute_all", "force_update":False})
    if kwargs['task'] == "compute_all":
        for id, vehicle_df in iterate_over_processed_ts(query_str=kwargs.get("query_str", None)):
            perfs_dict = compute_perfs(vehicle_df, id, force_update=kwargs["force_update"])

# ==============================PERFS DEPENDANT ON FLEET WISE DATA==============================

def compute_perfs(vehicle_df: DF, id:str, force_update=False) -> dict[str, DF]:
    return {
        **independant_perfs_of(vehicle_df, id, force_update=force_update),
        "energy_soh": compute_energy_soh(vehicle_df, id)
    }

def compute_energy_soh(vehicle_df:DF, id:str) -> Series:
    """
    ### Description:
    Computes the soh from the energy distribution specific to that vehicle and the fleet energy distribution.
    ### Returns:
    Series of soh values indexed by odometer range.
    """
    raw_vehicle_charge_energy_dist = charge_energy_points_of(vehicle_df, id)
    soh_over_odometer_intervals = (
        raw_vehicle_charge_energy_dist
        .pipe(compute_charge_energy_dist)
        .groupby(level=0)
        .apply(yes)
        .groupby(level=0, sort=True)
        .mean()
        .mul(100)
    )

    # print(soh_over_odometer_intervals.values)

    soh = DF({
        "soh": soh_over_odometer_intervals,
        "mean_odo": soh_over_odometer_intervals.index,
    })

    return soh

def yes(dist_grp: Series) -> Series:
    dist_grp = dist_grp.droplevel(0)
    intersecting_index = dist_grp.index.intersection(default_100_soh_charge_energy_dist.index)
    numerator = dist_grp.loc[intersecting_index]
    denumerator = default_100_soh_charge_energy_dist.loc[intersecting_index]

    return numerator / denumerator

# =====================================INDEPENDANT PERFS========================================

def independant_perfs_of(vehicle_df: DF, id:str, force_update=False) -> dict[str, DF]:
    return {
        # "discharge": compute_discharge_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC),
        "charge": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge", "energy_soh"),
        "charge_above_80": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge_above_80", "energy_soh_above_80"),
        "charge_bellow_80": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge_bellow_80", "energy_soh_bellow_80"),
        "energy_distribution_per_soc": charge_energy_points_of(vehicle_df, id, force_update=force_update)
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

def charge_energy_points_of(vehicle_df: DF, id:str, force_update=False) -> DF:
    return data_caching_wrapper(
        id,
        PATH_TO_CHARGING_PERF_PER_SOC.format(id=id),
        lambda _: compute_charge_energy_points_df(vehicle_df),
        force_update,
    )

def compute_charge_energy_points_df(vehicle_df:DF) -> DF:
    """
    ### Description:
    This function computes the distribution of required energy to gain one 0.5% soc over:
    - odometer intervals of width ODOMETER_FLOOR_RANGE_FOR_ENERGY_DIST
    - temperature intervals of width TEMP_FLOOR_RANGE_FOR_ENERGY_DIST
    - soc (yes, the energy required to gain one soc also depends on the current soc)
    # Returns:
    Dataframe multi indexed by odometer range, temp range, soc. 
    Main column is energy_added, the other are not really important
    """
    return (
        vehicle_df
        .pipe(
            perfs.agg_diffs_df_of,
            {
                "cum_energy": "energy_added",
                "battery_range_km": "range_gained",
                "power": "power_diff",
            },
            "in_charge_perf_mask",
            [
                vehicle_df["odometer"].ffill().floordiv(ODOMETER_FLOOR_RANGE_FOR_ENERGY_DIST).mul(ODOMETER_FLOOR_RANGE_FOR_ENERGY_DIST),
                vehicle_df["temp"].ffill().floordiv(TEMP_FLOOR_RANGE_FOR_ENERGY_DIST).mul(TEMP_FLOOR_RANGE_FOR_ENERGY_DIST),
                vehicle_df["in_charge_perf_idx"],
                vehicle_df["soc"].ffill() 
            ]
        )
        .pipe(lambda df: df.assign(energy_added=df["energy_added"].replace(0, np.nan)))
        .drop(columns=COLS_TO_DROP_FOR_ENERGY_DISTRIBUTION)
        .groupby(level=[0, 1, 3])
        .agg("mean")
        .sort_index()
        .eval("energy_added = energy_added * -1")
        .eval("power = energy_added / sec_duration")
    )

if __name__ == '__main__':
    main()
