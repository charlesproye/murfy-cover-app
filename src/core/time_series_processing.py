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

def process_date(ts: DF, add_sec_time_diff_col=False, original_name="date", set_as_index=False) -> DF:
    """
    ### Description:
    Applies all the boiler plate operation on the "date" column:
    1. Sets the unit as second for easier convertion into timestamps.
    1. Drops rows with duplicate timestamps.
    1. Sorts the rows 
    ### Parameters:
    original_name, str, default="date":   
        Name of the column that represents the date before parsing, will be renamed to date.    
        If the original_name is not present in the dataframe, the function returns the dataframe untoched.
    add_sec_time_diff_col, bool:  
        Set this to true to add an int "sec_time_diff" column  
    set_as_index, bool:  
    Set to True if you want the date to becaome the index, will not drop the column.  
    """
    if not original_name in ts.columns:
        return ts
    ts = (
        ts
        .rename(columns={original_name: "date"})
        .assign(date=pd.to_datetime(ts["date"]).dt.as_unit("s"))
        .drop_duplicates("date")
        .sort_index()
    )
    if set_as_index:
        ts = ts.set_index("date", drop=False)
    if add_sec_time_diff_col:
        ts["sec_time_diff"] = (
            ts["date"]
            .ffill()
            .diff()
            .dt.as_unit("s")
            .astype(int)
        )

    return ts

def compute_cum_integrals_of_current_vars(vehicle_df: DF) -> DF:
    """
    ### Description:
    Computes and adds to the dataframe cumulative energy (in kwh) and charge (in C).
    """
    if "power" in vehicle_df.columns:
        vehicle_df["cum_energy"] = cum_integral(vehicle_df["power"], date_series=vehicle_df["date"])
    if "current" in vehicle_df.columns:
        vehicle_df["cum_charge"] = cum_integral(vehicle_df["current"], date_series=vehicle_df["date"])
    return vehicle_df

def cum_integral(power_series: Series, date_series=None) -> Series:
    """
    ### Description:
    Computes the cumulative energy of the time series by using cumulative trapezoid intergrating of the power column.
    ### Parameters:
    power_col: name of the power column, must be in kw.
    date_series: optional parameter to provide if the series is not indexed by date.
    ### Returns:
    df with added column with the name of cum_energy_col in kWh.
    """
    date_series = power_series.index.to_series() if date_series is None else date_series
    cum_energy_data = integrate.cumulative_trapezoid(
        # Make sure that date time units are in seconds before converting to int
        x=date_series.dt.as_unit("s").astype(int),
        y=power_series.fillna(0).values,
        initial=0,
    )
    return Series(cum_energy_data * KJ_TO_KWH, index=power_series.index)


def in_motion_mask_from_odo_diff(vehicle_df: DF) -> DF:
    return (
        vehicle_df
        # use interpolate before checking if the odometer increased to compensate for missing values
        .assign(in_motion=vehicle_df["odometer"].interpolate(method="time").diff().gt(0))
        .pipe(perf_mask_and_idx_from_condition_mask, "in_motion")
    )

def high_freq_in_discharge_and_charge_from_soc_diff(vehicle_df: DF) -> DF:
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
    vehicle_df = (
        vehicle_df
        .eval("in_discharge = soc_dir == -1")
        .eval("in_charge = soc_dir == 1")
    ) 

    return vehicle_df


# TODO: Find why some perfs grps have a size of 1 even though they are supposed to be filtered out with  trimed_series if trimed_series.sum() > 1 else False
def perf_mask_and_idx_from_condition_mask(vehicle_df: DF, src_mask:str, src_mask_idx_col_name="{src_mask}_idx", perf_mask_col_name="{src_mask}_perf_mask", max_time_diff:TD|None=None) -> DF:
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
    idx = mask_for_idx.cumsum()
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
