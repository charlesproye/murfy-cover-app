from logging import getLogger
from datetime import timedelta as TD

import numpy as np
from scipy.integrate import cumulative_trapezoid

from core.config import *
from core.constants import *
from core.pandas_utils import *


logger = getLogger("core.time_series_processing")

def compute_discharge_diffs(tss:DF, vars_to_measure:list[str], id_col:str="vin", logger:Logger=logger) -> DF:
    logger.info(f"compute_discharge_summary called.")
    if id_col not in tss.columns or "in_discharge_perf_idx" not in tss.columns:
        logger.warning(f"{id_col} or in_discharge_perf_idx column not found, returning tss unchanged.")
        logger.warning("columns:\n{}".format('\n'.join(tss.columns)))
        return tss
    for var in tss.columns.intersection(vars_to_measure):
        logger.debug(f"transforming {var}")
        tss[f"{var}_discharge_loss"] = tss.groupby([id_col, "in_discharge_perf_idx"])[var].transform(series_start_end_diff)

    return tss

def fillna_vars(tss:DF, vars_to_fill:list[str], max_time_diff:TD|None, id_col:str=None, logger:Logger=logger) -> DF:
    logger.info(f"fillna_vars called.")
    time_diff_low_enough_to_ffill = tss.groupby(id_col)["date"].diff() < max_time_diff
    for var in tss.columns.intersection(vars_to_fill):
        logger.debug(f"- filling {var}")
        var_filled = tss.groupby(id_col)[var].ffill().groupby(tss[id_col]).bfill()
        tss[f"ffilled_{var}"] = tss[var].mask(time_diff_low_enough_to_ffill & tss[var].isna(), var_filled)

    return tss

def compute_cum_var(tss: DF, var_col:str, cum_var_col:str, id_col:str="vin", logger:Logger=logger) -> DF:
    if var_col in tss.columns:
        logger.debug(f"Computing {cum_var_col} from {var_col}.")
        tss[cum_var_col] = (
            cumulative_trapezoid(
                # Leave the keywords as default order is y x not x y (-_-)
                # Make sure that date time units are in seconds before converting to int
                x=tss["date"].dt.as_unit("s").astype(int),
                y=tss[var_col].fillna(0).values,
                initial=0,
            )            
            .astype("float32")
        )
        tss[cum_var_col] *= KJ_TO_KWH # Convert from kj to kwh
        # Reset value to zero at the start of each vin time series
        tss[cum_var_col] -= tss.groupby(id_col)[cum_var_col].transform("first")
    else:
        logger.debug(f"{var_col} not found, not computing {cum_var_col}.")
    return tss

def high_freq_in_discharge_and_charge_from_soc_diff(ts: DF) -> DF:
    """If the time series has more than 6 hours in between soc points, use `low_freq_in_discharge_and_charge_from_soc_diff."""
    ts["soc_dir"] = norm_soc_dir(ts["soc"].ffill().diff())
    # mitigate soc spikes effect on mask
    prev_dir = ts["soc_dir"].ffill().shift()
    next_dir = ts["soc_dir"].bfill().shift(-1)
    ts["value_is_spike"] = (next_dir == prev_dir) & (ts["soc_dir"] != next_dir) & ts["soc_dir"].notna()
    ts["soc_dir"] = ts["soc_dir"].mask(ts["value_is_spike"], np.nan)
    ts["smoothed_soc_dir"] = (
        ts["soc_dir"]
        .rolling(window=TD(minutes=20), center=True)
        .mean()
        .pipe(np.sign)
    )
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

def norm_soc_dir(soc_dir:Series) -> Series:
    """Normalize the soc direction to -1 for negative, NaN for zero, 1 for positive."""
    return soc_dir / soc_dir.abs()

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

