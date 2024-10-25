from datetime import timedelta as TD

import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from pandas.api.typing import DataFrameGroupBy as DF_grp_by
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from scipy import integrate
from rich import print
import numpy as np

from core.config import *
from core.constants import *

def estimate_dummy_soh(ts: DF, soh_lost_per_km_ratio:float=SOH_LOST_PER_KM_DUMMY_RATIO) -> DF:
    """
    ### Description:
    Estimates an soh according to the odometer and a soh loss per km ratio.
    Expects the odometer to be in km.
    Very inacurrate but very handy when your deadline has been advanced from 3 months to 3 days...
    """
    ts["soh"] = 100 - ts["odometer"].mul(soh_lost_per_km_ratio).ffill()

    return ts


def compute_cum_energy(vehicle_df: DF) -> DF:
    """
    ### Description:
    Computes and adds to the dataframe cumulative energy (in kwh) and charge (in C).
    """
    if "power" in vehicle_df.columns:
        vehicle_df["cum_energy"] = (
            vehicle_df["power"]
            .pipe(cum_integral, date_series=vehicle_df["date"])
            .astype("float32")
        )
    return vehicle_df

def cum_integral(series: Series, date_series=None) -> Series:
    """
    ### Description:
    Computes the cumulative of the time series by using cumulative trapezoid and a date series.
    ### Parameters:
    power_col: name of the power column, must be in kw.
    date_series: optional parameter to provide if the series is not indexed by date.
    """
    date_series = series.index.to_series() if date_series is None else date_series
    cum_energy_data = integrate.cumulative_trapezoid(
        # Make sure that date time units are in seconds before converting to int
        x=date_series.dt.as_unit("s").astype(int),
        y=series.fillna(0).values,
        initial=0,
    )
    return Series(cum_energy_data * KJ_TO_KWH, index=series.index)

def low_freq_mask_in_motion_periods(ts:DF) -> DF:
    """Use for time series where there can be more than 6 hours in between odometer points"""
    if not isinstance(ts.index, pd.core.indexes.datetimes.DatetimeIndex):
        ts = ts.set_index("date", drop=False)
    return (
        ts #
        .assign(odometer_increase_mask=ts["odometer"].interpolate(method="time").diff().gt(0.0, fill_value=True))
        .assign(last_notna_odo_date=ts["date"].mask(ts["odometer"].isna(), pd.NaT).ffill())
        .assign(time_diff_low_enough=lambda ts: ts["last_notna_odo_date"].diff().lt(pd.Timedelta("6h")))
        .eval("in_motion = odometer_increase_mask & time_diff_low_enough")
        .pipe(perf_mask_and_idx_from_condition_mask, "in_motion")
    )


def low_freq_compute_charge_n_discharge_vars(ts:DF) -> DF:
    """Use for time series where there can be more than 6 hours in between soc points"""
    MAX_CHARGE_TIME_DIFF = TD(hours=6)
    ts = (
        ts
        .set_index("date", drop=False)
        .pipe(high_freq_in_discharge_and_charge_from_soc_diff)
    )
    ts["last_notna_soc_date"] = ts["date"].mask(ts["soc"].isna(), pd.NaT).shift().ffill()
    ts["last_notna_soc_diff_low_enough"] = ts.eval("date - last_notna_soc_date").lt(MAX_CHARGE_TIME_DIFF)
    ts["date_diff_low_enough"] = ts["date"].diff().lt(MAX_CHARGE_TIME_DIFF)
    ts["in_charge"] = ts.eval("in_charge & last_notna_soc_diff_low_enough & date_diff_low_enough")
    ts["in_discharge"] = ts.eval("in_discharge & last_notna_soc_diff_low_enough & date_diff_low_enough")
    ts = (
        ts
        .pipe(perf_mask_and_idx_from_condition_mask, "in_charge")
        .pipe(perf_mask_and_idx_from_condition_mask, "in_discharge")
    )

    return ts


def high_freq_in_motion_mask_from_odo_diff(vehicle_df: DF) -> DF:
    """If the time series has more than 6 hours in between soc points, use `low_freq_mask_in_motion_periods`."""
    return (
        vehicle_df
        # use interpolate before checking if the odometer increased to compensate for missing values
        .assign(in_motion=vehicle_df["odometer"].interpolate(method="time").diff().gt(0))
        .pipe(perf_mask_and_idx_from_condition_mask, "in_motion")
    )

def high_freq_in_discharge_and_charge_from_soc_diff(vehicle_df: DF) -> DF:
    """If the time series has more than 6 hours in between odometer points, use `high_freq_in_discharge_and_charge_from_soc_diff`."""
    soc_diff = vehicle_df["soc"].ffill().diff()
    vehicle_df["soc_dir"] = np.nan
    vehicle_df["soc_dir"] = (
        vehicle_df["soc_dir"] 
        .mask(soc_diff.gt(0), 1)
        .mask(soc_diff.lt(0), -1)
    )
    # mitigate soc spikes effect on mask
    prev_dir = vehicle_df["soc_dir"].ffill().shift()
    next_dir = vehicle_df["soc_dir"].bfill().shift(-1)
    vehicle_df["value_is_spike"] = (next_dir == prev_dir) & (vehicle_df["soc_dir"] != next_dir) & vehicle_df["soc_dir"].notna()
    vehicle_df["soc_dir"] = vehicle_df["soc_dir"].mask(vehicle_df["value_is_spike"], np.nan)
    vehicle_df["smoothed_soc_dir"] = vehicle_df["soc_dir"].rolling(window=TD(minutes=20), center=True).mean()
    vehicle_df["soc_dir"] = (
        vehicle_df["soc_dir"]
        .mask(vehicle_df["smoothed_soc_dir"].gt(0) & vehicle_df["soc_dir"].lt(0), np.nan)
        .mask(vehicle_df["smoothed_soc_dir"].lt(0) & vehicle_df["soc_dir"].gt(0), np.nan)
    )

    bfilled_dir = vehicle_df["soc_dir"].bfill()
    ffilled_dir = vehicle_df["soc_dir"].ffill()
    vehicle_df["soc_dir"] = vehicle_df["soc_dir"].mask(bfilled_dir == ffilled_dir, ffilled_dir)
    vehicle_df = vehicle_df.eval("in_discharge = soc_dir == -1")
    vehicle_df = vehicle_df.eval("in_charge = soc_dir == 1")
    vehicle_df = vehicle_df.drop(columns=["soc_dir", "smoothed_soc_dir"])

    return vehicle_df


# TODO: Find why some perfs grps have a size of 1 even though they are supposed to be filtered out with  trimed_series if trimed_series.sum() > 1 else False
def perf_mask_and_idx_from_condition_mask(
        vehicle_df: DF,
        src_mask:str,
        src_mask_idx_col_name="{src_mask}_idx",
        perf_mask_col_name="{src_mask}_perf_mask",
        max_time_diff:TD|None=None
    ) -> DF:
    src_mask_idx_col_name = src_mask_idx_col_name.format(src_mask=src_mask)
    perf_mask_col_name = perf_mask_col_name.format(src_mask=src_mask)
    vehicle_df[src_mask_idx_col_name] = period_idx_of_mask(vehicle_df, src_mask, max_time_diff=max_time_diff)
    perf_grps = vehicle_df.groupby(src_mask_idx_col_name)["soc"]
    vehicle_df[perf_mask_col_name] = perf_grps.transform(sanitize_perf_period) & vehicle_df[src_mask]
    vehicle_df[f"{src_mask}_perf_idx"] = period_idx_of_mask(vehicle_df, perf_mask_col_name)

    return vehicle_df

def period_idx_of_mask(vehicle_df:DF, mask_col: str, period_shift:int=1, max_time_diff:TD|None=None) -> Series:
    if max_time_diff:
        mask = vehicle_df.eval(f"{mask_col} & sec_time_diff < {max_time_diff.total_seconds()}") 
    else:
        mask = vehicle_df[mask_col]
    mask_for_idx = mask.ne(mask.shift(period_shift), fill_value=False)
    idx = mask_for_idx.cumsum().astype("uint16")
    return idx

def sanitize_perf_period(soc: Series, min_size=2) -> bool|Series:
    trimed_mask = trim_off_mask_perf_period(soc)
    return trimed_mask if trimed_mask.sum() >= min_size else False

def trim_off_mask_perf_period(soc: Series) -> Series:
    return mask_off_trailing_soc(soc) & mask_off_leading_soc(soc)

def mask_off_trailing_soc(soc: Series) -> Series:
    ffilled_soc = soc.ffill()
    return ffilled_soc.ne(ffilled_soc.iat[-1]).shift(fill_value=True)

def mask_off_leading_soc(soc: Series) -> Series:
    bfilled_soc = soc.bfill()
    bfilled_first = bfilled_soc.iat[0]
    return bfilled_soc.ne(bfilled_first).shift(-1, fill_value=True)
