# Currently this is in watea but it's destined to be moved to core when we will use it for tesla
from pandas import DataFrame as DF
from pandas import Series
import pandas as pd
import numpy as np

import core.perf_agg_processing as perfs
from core.pandas_utils import floor_to, series_start_end_diff, log_data_and_return_same_data
from watea.watea_constants import *
from watea.watea_fleet_info import fleet_info_df

def soh_from_intercepts(vehicle_charge_dist_intercept: Series, default_100_soh_intercepts: Series) -> Series:
    return (
        vehicle_charge_dist_intercept
        .div(default_100_soh_intercepts, level=1)
        .mul(100)
        .groupby(level=0)
        .median()
    )

def dists_from_default_dist_and_intercepts(intercepts: Series, default_dist_shape: Series) -> Series:
    """
    ### Descriptoion:
    Creates a synthetic energy distribution that is the sum of the default distribution shape + the intercepts.
    """
    return (
        intercepts
        .groupby(level=[0, 1])
        .apply(lambda intercept: Series((default_dist_shape + intercept.iat[0]).values, index=default_dist_shape.index)) 
    )

# TODO: Try to merge the two diffs functions
def all_diffs_from_points_to_dist(medians: Series, dist_shape) -> Series:
    return (
        medians
        .groupby(level=[0, 1])
        .apply(diff_from_points_to_dist, dist_shape)
    )
# TODO: Change the soc aggregation in compute_charge_energy_points_df to 0.5 soc and remove the roudning operation here
# Use .sub(dist_shape).median instead of np.median(median.values - dist_shape.loc[idx]) 
# Convert the entire function into chain calling style 
def diff_from_points_to_dist(median: Series, dist_shape: Series) -> float:
    median = median.drop_duplicates()
    idx = median.index.get_level_values(2) #(median.index.get_level_values(2) * 2).round() / 2
    
    return np.median(median.values - dist_shape.loc[idx]) 

def fit_poly_lr_to_charge_dist_xs(dist_median_xs: DF) -> Series:
    charge_energy_distribution = (
        CHARGE_ENERGY_POINTS_TO_DIST_MODEL
        .fit(dist_median_xs.index.values, dist_median_xs.values)
        .predict(VOLTAGE_RANGE)
        .squeeze()
    )
    fitted_fleet_charge_energy_dist_xs = (
        Series(charge_energy_distribution, VOLTAGE_RANGE)
        .clip(300, 500)
    )

    return fitted_fleet_charge_energy_dist_xs

def compute_charge_energy_median(charge_energy_points_df: DF) -> pd.Series:
    return (
        charge_energy_points_df
        .query(MOST_COMMON_CHARGE_REGIME_QUERY)
        .loc[:, "energy_added"]
        .groupby(level=[0, 1, 2])
        .agg("median")
        .groupby(level=[0, 1])
        .rolling(4, center=True, min_periods=1)
        .median()
        .dropna()
        .droplevel([0, 1])
    )

def aggregate_energy_points_across_charges(energy_points:DF) -> DF:
    return (
        energy_points
        .groupby(level=[0, 1, 3])
        .agg("mean")
        .drop(columns=["charge_idx", "charge_id"])
    )

def compute_charging_points(ts:DF, id:str) -> DF:
    # Use the index/location of the id of the ts in the fleet_info_df to track back the origin of energy points during debugging
    # TODO: use hashing or another method that is only dependent on the id instead of the location in the fleet info order
    id_idx = fleet_info_df.index.get_loc(id) 
    return (
        ts
        .assign(ffilled_odometer=ts["odometer"].ffill())
        .assign(floored_soc=floor_to(ts["soc"].ffill(), ENERGY_POINTS_GRP_BY_SOC_QUANTIZATION))
        .query("in_charge_perf_mask")
        .groupby(["in_charge_perf_idx","floored_soc",], sort=True)
        # TODO: Use pd.NamedAgg & agg kwargs instead of agg & rename
        .agg(
            odometer=pd.NamedAgg("ffilled_odometer", "mean"),
            energy_added=pd.NamedAgg("cum_energy", series_start_end_diff),
            voltage=pd.NamedAgg("voltage", "median"),
            current=pd.NamedAgg("current", "median"),
            temperature=pd.NamedAgg("temp", "median"),
            sec_duration=pd.NamedAgg("date", lambda s: series_start_end_diff(s).total_seconds()),
            date=pd.NamedAgg("date", lambda s: s.iat[0]),
            soc=pd.NamedAgg("floored_soc", "mean"),
        )
        .assign(energy_added=lambda df: df["energy_added"].replace(0, np.nan))
        # Deubgging
        .assign(id=id)
        .assign(charge_idx=lambda df: df.index.get_level_values(0))
        .assign(id_idx=id_idx)
        .assign(charge_id= lambda df: id + "_"  + df["charge_idx"].astype("string"))
        .eval("energy_added = energy_added * -1")
        .eval("current = current * -1")
    )

