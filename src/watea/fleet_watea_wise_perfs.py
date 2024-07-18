import pandas as pd
from pandas import DataFrame as DF
import numpy as np
from rich import print

from watea.watea_constants import *
from watea.processed_watea_ts import iterate_over_processed_ts
from core.caching_utils import data_caching_wrapper
from core.pandas_utils import *

def get_fleet_charge_energy_points_df(force_update=False, **kwargs) -> DF:
    return data_caching_wrapper(
        "",
        PATH_TO_FLEET_WISE_DISTRIBUTION,
        lambda _: compute_fleet_wise_energy_distribution(force_update=force_update, **kwargs),
        force_update=force_update
    )

def compute_fleet_wise_energy_distribution(force_update=False, query_str="has_power_during_charge") -> DF:
    from watea.watea_perfs import charge_energy_points_of

    fleet_data_df: dict[str, DF] = {}
    for id, vehicle_df in iterate_over_processed_ts(query_str=query_str):
        distribution_df = charge_energy_points_of(vehicle_df, id, force_update=force_update)
        fleet_data_df[id] = distribution_df
    fleet_data_df: DF = pd.concat(fleet_data_df, axis='index')
    fleet_data_df.index.names = ["id", "odometer", "temp", "soc"]
    fleet_data_df = (
        fleet_data_df
        .sort_index()
        .droplevel(0)
    )

    return fleet_data_df

def compute_charge_energy_dist(charge_energy_points_df: DF) -> pd.Series:
    """
    ### Description:
    Processes the raw distribution to obtain a single 
    """
    return (
        charge_energy_points_df
        .query("energy_added > 300 & energy_added < 500 & sec_duration < 900 & temp < 35 & power < 4 & power > 1.5")
        .loc[:, "energy_added"]
        .groupby(level=[0, 1, 2])
        .agg("median")
        .groupby(level=[0, 1])
        .apply(compute_median_energy_dist_per_soc_of_xs)
    )

def compute_median_energy_dist_per_soc_of_xs(dist_grp: Series) -> DF:
    return (
        dist_grp
        .rolling(4, center=True)
        .median()
        .mask(lambda series: series < series.cummax(), pd.NA)
        .interpolate()
        .rolling(10, center=True)
        .mean()
        .droplevel([0, 1])
    )

fleet_charge_energy_points_df = get_fleet_charge_energy_points_df()
default_charge_energy_dist = compute_charge_energy_dist(fleet_charge_energy_points_df)
default_100_soh_charge_energy_dist = default_charge_energy_dist.xs(0, level=0)
if __name__ == "__main__":
    print(default_charge_energy_dist.index)
    print(default_100_soh_charge_energy_dist.index)

    # print(default_100_soh_charge_energy_dist.to_string(max_rows=None))
