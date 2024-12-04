from core.pandas_utils import *
from datetime import timedelta as TD
from logging import getLogger
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from scipy import integrate
import numpy as np
from core.config import *
from core.constants import *

logger = getLogger("core.time_series_processing")

def compute_discharge_summary(tss:DF, logger:Logger=logger) -> DF:
    logger.info(f"compute_discharge_summary called.")
    return (
        tss
        .query("in_discharge_perf_mask")
        .groupby(["vin", "in_discharge_perf_idx"])
        .transform({
            "soc": series_start_end_diff,
            "odometer": series_start_end_diff,
            "estimated_range": series_start_end_diff,
        })
    )

def compute_charging_n_discharging_masks(tss:DF, id_col:str=None, charging_status_val_to_mask:dict=None, logger:Logger=logger) -> DF:
    """
    ### Description:
    Computes the charging and discharging masks for a time series.
    Uses the string charging_status column if it exists, otherwise uses the soc difference.
    ### Parameters:
    id_col: optional parameter to provide if the dataframe represents multiple time series.
    charging_status_val_to_mask: dict mapping charging status values to boolean values to create masks.
    """
    logger.info(f"compute_charging_n_discharging_masks called.")
    if "charging_status" in tss.columns and charging_status_val_to_mask is not None:
        logger.debug(f"Computing charging and discharging masks using charging status dictionary.")
        charge_mask = tss["charging_status"].map(charging_status_val_to_mask)
        deischarge_mask = charge_mask == False
        tss["in_charge"] = charge_mask
        tss["in_discharge"] = charge_mask == False
        return tss
    elif "soc" in tss.columns:
        logger.debug(f"Computing charging and discharging masks using soc difference.")
        if id_col in tss.columns:
            return (
                tss
                .groupby(id_col)
                .apply(low_freq_compute_charge_n_discharge_vars)
                .reset_index(drop=True)
            )
        else:
            return low_freq_compute_charge_n_discharge_vars(tss)
    else:
        logger.warning("No charging status or soc column found to compute charging and discharging masks, returning original tss.")
        return tss

def compute_cum_energy(vehicle_df: DF, power_col:str="power", cum_energy_col:str="cum_energy") -> DF:
    """
    ### Description:
    Computes and adds to the dataframe cumulative energy (in kwh) and charge (in C).
    """
    if power_col in vehicle_df.columns:
        vehicle_df[cum_energy_col] = (
            vehicle_df[power_col]
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
    ts["odometer_increase_mask"] = ts["odometer"].interpolate(method="time").diff().gt(0.0, fill_value=True)
    ts["last_notna_odo_date"] = ts["date"].mask(ts["odometer"].isna(), pd.NaT).ffill()
    ts["time_diff_low_enough"] = ts["last_notna_odo_date"].diff().lt(pd.Timedelta("6h"))
    ts["in_motion"] = ts["odometer_increase_mask"] & ts["time_diff_low_enough"]
    ts = perf_mask_and_idx_from_condition_mask(ts, "in_motion")

    return ts


def low_freq_compute_charge_n_discharge_vars(ts:DF) -> DF:
    """Use for time series where there can be more than 6 hours in between soc points"""
    MAX_CHARGE_TIME_DIFF = TD(hours=6)
    date_not_index = not isinstance(ts.index, pd.core.indexes.datetimes.DatetimeIndex)
    if date_not_index:
        ts = ts.set_index("date", drop=False)
        ts = ts.sort_index()
    ts = ts.pipe(high_freq_in_discharge_and_charge_from_soc_diff)
    ts["last_notna_soc_date"] = ts["date"].mask(ts["soc"].isna(), pd.NaT).shift().ffill()
    ts["last_notna_soc_diff_low_enough"] = ts.eval("date - last_notna_soc_date").lt(MAX_CHARGE_TIME_DIFF)
    ts["date_diff_low_enough"] = ts["date"].diff().lt(MAX_CHARGE_TIME_DIFF)
    ts["in_charge"] = ts.eval("in_charge & last_notna_soc_diff_low_enough & date_diff_low_enough")
    ts["in_discharge"] = ts.eval("in_discharge & last_notna_soc_diff_low_enough & date_diff_low_enough")
    ts = perf_mask_and_idx_from_condition_mask(ts, "in_charge")
    ts = perf_mask_and_idx_from_condition_mask(ts, "in_discharge")
    if date_not_index:
        ts = ts.reset_index(drop=True)
    return ts


def high_freq_in_motion_mask_from_odo_diff(vehicle_df: DF) -> DF:
    """If the time series has more than 6 hours in between soc points, use `low_freq_mask_in_motion_periods`."""
    return (
        vehicle_df
        # use interpolate before checking if the odometer increased to compensate for missing values
        .assign(in_motion=vehicle_df["odometer"].interpolate(method="time").diff().gt(0))
        .pipe(perf_mask_and_idx_from_condition_mask, "in_motion")
    )

def high_freq_in_discharge_and_charge_from_soc_diff(ts: DF) -> DF:
    """If the time series has more than 6 hours in between odometer points, use `high_freq_in_discharge_and_charge_from_soc_diff`."""
    soc_diff = ts["soc"].ffill().diff()
    ts["soc_dir"] = np.nan
    ts["soc_dir"] = (
        ts["soc_dir"] 
        .mask(soc_diff.gt(0), 1)
        .mask(soc_diff.lt(0), -1)
    )
    # mitigate soc spikes effect on mask
    prev_dir = ts["soc_dir"].ffill().shift()
    next_dir = ts["soc_dir"].bfill().shift(-1)
    ts["value_is_spike"] = (next_dir == prev_dir) & (ts["soc_dir"] != next_dir) & ts["soc_dir"].notna()
    ts["soc_dir"] = ts["soc_dir"].mask(ts["value_is_spike"], np.nan)
    ts["smoothed_soc_dir"] = ts["soc_dir"].rolling(window=TD(minutes=20), center=True).mean()
    ts["soc_dir"] = (
        ts["soc_dir"]
        .mask(ts["smoothed_soc_dir"].gt(0) & ts["soc_dir"].lt(0), np.nan)
        .mask(ts["smoothed_soc_dir"].lt(0) & ts["soc_dir"].gt(0), np.nan)
    )

    bfilled_dir = ts["soc_dir"].bfill()
    ffilled_dir = ts["soc_dir"].ffill()
    ts["soc_dir"] = ts["soc_dir"].mask(bfilled_dir == ffilled_dir, ffilled_dir)
    ts = ts.eval("in_discharge = soc_dir == -1")
    ts = ts.eval("in_charge = soc_dir == 1")
    ts = ts.drop(columns=["soc_dir", "smoothed_soc_dir"])

    return ts


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


def tss_frequency_sanity_check(tss:DF, date_col:str="date", id_col:str="vin") -> DF:
    """
    ### Description:
    Computes the frequency of the time series as the mean of the date difference.
    """
    describe_freq = lambda x: pd.to_datetime(x).drop_duplicates().sort_values().diff().dropna().describe()
    if id_col in tss.columns:
        return (
            tss
            .groupby(id_col)[date_col]
            .apply(describe_freq)
            .unstack(level=1)
        )
    else:
        return describe_freq(tss[date_col])

