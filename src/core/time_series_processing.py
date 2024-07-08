import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from pandas.api.typing import DataFrameGroupBy as DF_grp_by
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from scipy import integrate
from rich import print
import numpy as np

from .constant_variables import KJ_TO_KWH

def add_cum_energy_from_power_cols(vehicle_df: DF, power_col:str, energy_col:str) -> DF:
    vehicle_df[energy_col] = cum_energy_from_power(vehicle_df[power_col])
    return vehicle_df

def cum_energy_from_power(power_series: Series) -> Series:
    """
    ### Description:
    Computes the cumulative energy of the time series by using cumulative trapezoid intergrating of the power column.
    ### Parameters:
    power_col: name of the power column, must be in kw.
    ### Returns:
    df with added column with the name of cum_energy_col in kWh.
    """
    cum_energy_data = integrate.cumulative_trapezoid(
        # Make sure that date time units are in seconds before converting to int
        x=power_series.index.to_series().dt.as_unit("s").astype(int),
        y=power_series.values,
        initial=0,
    )
    return Series(cum_energy_data * KJ_TO_KWH, index=power_series.index)

def soh_from_est_battery_range(vehicle_df: DF, range_col: str, stock_km_per_soc: float, **kwargs) -> DF:
    """
    ### Description:
    Computes soh from estimated range of the car.
    The range column must be in kilometers.
    """
    vehicle_df["km_per_soc"] = vehicle_df[range_col] / vehicle_df["soc"]
    vehicle_df["range_soh"] = 100 * vehicle_df["km_per_soc"] / stock_km_per_soc
    vehicle_df["range_soh"] = vehicle_df["range_soh"].mask(vehicle_df["soc"].diff().eq(0), np.nan).interpolate(method="time")
    vehicle_df["smoothed_soh"] = double_rolling_median_smoothing(vehicle_df["range_soh"])
    
    return vehicle_df

def double_rolling_median_smoothing(src:Series, window:str|int="3h") -> Series:
    return (
        src
        .rolling(window, center=True).median()
        .rolling(window, center=True).median()
    )

def in_motion_mask_from_odo_diff(vehicle_df: DF) -> DF:
    return (
        vehicle_df
        .assign(in_motion=vehicle_df["odometer"].diff().ne(0, fill_value=False))
        .pipe(perf_mask_and_idx_from_condition_mask, "in_motion")
    )

def in_charge_and_discharge_mask_fromo_soc_diff(vheicle_df: DF) -> DF:
    return (
        vheicle_df
        .assign(soc_diff=vheicle_df["soc"].diff())
        .eval("in_charge = soc_diff > 0")
        .eval("in_discharge = soc_diff <= 0")
        .pipe(perf_mask_and_idx_from_condition_mask, "in_charge")
        # .pipe(perf_mask_and_idx_from_condition_mask, "in_discharge")
    )

def self_discharge(vehicle_df: DF) -> DF:
    return (
        vehicle_df
        .assign(in_self_discharge=~vehicle_df["in_motion"] & ~vehicle_df["in_charge"])
        .pipe(perf_mask_and_idx_from_condition_mask, "in_self_discharge")
    )

# TODO: Find why some perfs grps have a size of 1 even though they are supposed to be filtered out with  trimed_series if trimed_series.sum() > 1 else False
def perf_mask_and_idx_from_condition_mask(vehicle_df: DF, src_mask:str) -> DF:
    src_mask_idx_col_name = f"{src_mask}_idx"
    perf_mask = f"{src_mask}_perf_mask"
    vehicle_df[src_mask_idx_col_name] = period_idx_of_mask(vehicle_df[src_mask])
    vehicle_df[perf_mask] = vehicle_df.groupby(src_mask_idx_col_name)["soc"].transform(sanitize_perf_period) & vehicle_df[src_mask]
    vehicle_df[f"{src_mask}_perf_idx"] = period_idx_of_mask(vehicle_df[perf_mask])

    return vehicle_df

def period_idx_of_mask(mask: Series, period_shift:int=1) -> Series:
    mask_for_idx = mask.ne(mask.shift(period_shift), fill_value=False)
    idx = mask_for_idx.cumsum()
    return idx

def sanitize_perf_period(soc: Series) -> bool|Series:
    trimed_mask = trim_off_mask_perf_period(soc)
    return trimed_mask if trimed_mask.sum() > 1 else False

def trim_off_mask_perf_period(soc: Series) -> Series:
    return mask_off_trailing_soc(soc) & mask_off_leading_soc(soc)

def mask_off_trailing_soc(soc: Series) -> Series:
    ffilled_soc = soc.ffill()
    return ffilled_soc.ne(ffilled_soc.iat[-1]).shift(fill_value=True)

def mask_off_leading_soc(soc: Series) -> Series:
    bfilled_soc = soc.bfill()
    bfilled_first = bfilled_soc.iat[0]
    return bfilled_soc.ne(bfilled_first).shift(-1, fill_value=True)
