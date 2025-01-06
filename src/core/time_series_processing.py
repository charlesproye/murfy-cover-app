from logging import getLogger

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

def describe_tss_date_diff(tss:DF, date_col:str="date", id_col:str="vin") -> DF:
    """
    ### Description:
    Computes the frequency of the time series as the mean of the date difference.
    """
    def describe_freq(date:Series) -> Series:
        return (
            pd.to_datetime(date)
            .drop_duplicates()
            .sort_values()
            .dropna()
            .diff()
            .describe()
        )
    return (
        tss
        .groupby(id_col)[date_col]
        .apply(describe_freq)
        .unstack(level=1)
    )

