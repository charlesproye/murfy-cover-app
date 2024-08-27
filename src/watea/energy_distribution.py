"""
This module implements the energy_soh pipeline. 
"""
# Currently this is in watea but it's destined to be moved to core when we will use it for tesla
from pandas import DataFrame as DF
from pandas import Series
import pandas as pd
import numpy as np

import core.perf_agg_processing as perfs
from watea.watea_constants import *

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
    idx = (median.index.get_level_values(2) * 2).round() / 2
    
    return np.median(median.values - dist_shape.loc[idx]) 

def fit_poly_lr_to_charge_dist_xs(dist_median_xs: DF) -> Series:
    charge_energy_distribution = (
        CHARGE_ENERGY_POINTS_TO_DIST_MODEL
        .fit(dist_median_xs.index.values, dist_median_xs.values)
        .predict(SOC_RANGE)
        .squeeze()
    )
    fitted_fleet_charge_energy_dist_xs = (
        Series(charge_energy_distribution, SOC_RANGE)
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

def charging_points_of(vehicle_df:DF) -> DF:
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
                vehicle_df["soc"].ffill(),
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
