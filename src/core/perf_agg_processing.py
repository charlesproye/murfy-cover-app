"""
This module provides functions to compute periods from vehicle time series that are common to any data provider.
Currently this is only for tesla vehicles but this will most likely become one or more base class(es) that specific data handlers/ETL will inherit from.
"""

import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from pandas.api.typing import DataFrameGroupBy as DF_grp_by

from core.constant_variables import DEFAULT_DIFF_VARS

def self_discharge_df_of(vehicle_df: DF, stock_kwh_per_soc: float, cum_energy_spent_col:str="cum_energy_spent") -> DF:
    return (
        vehicle_df
        .pipe(agg_diffs_df_of, {cum_energy_spent_col: "energy_diff"}, "in_self_discharge",)
        .pipe(compute_soh_from_soc_and_energy_diff, "energy_diff", stock_kwh_per_soc, "self_discharge_soh")
        .eval("secs_per_soc = sec_duration / soc_diff")
    )

def motion_perfs_df_of(vehicle_df: DF, stock_kwh_per_soc: float, cum_energy_spent_col:str="cum_energy_spent") -> DF:
    return (
        vehicle_df
        .pipe(agg_diffs_df_of, {cum_energy_spent_col: "energy_diff"}, "in_motion")
        .pipe(compute_soh_from_soc_and_energy_diff, "energy_diff", stock_kwh_per_soc, "motion_energy_soh")
        .eval("km_per_soc = distance / soc_diff")
    )

def compute_soh_from_soc_and_energy_diff(perf_df: DF, kwh_energy_col: str, stock_kwh_per_soc, soh_col) -> DF:
    """
    ### Description:
    Computes the soh estimation based on a kWh diff, soc diff and stock kwh per soc ratio.
    Make sure the energy diff is in kwh.
    """
    return (
        perf_df
        .eval(f"kwh_per_soc = {kwh_energy_col} / soc_diff")
        .eval(f"{soh_col} = kwh_per_soc / {stock_kwh_per_soc}")
    )

def agg_diffs_df_of(vehicle_df: DF, get_start_end_diff_args: dict[str, str], query:str, grp_by=None) -> DF:
    """
    ### Description:
    Computes the "base dataframe" for perf calculation based on diffrences.  
    The df is created from a subset of groups with `vehicle_df.query(query).groupby(grp_by)`.    
    For each key/value in the `get_start_end_diff_args` dict, `agg_diffs_df_of` computes a column:
    - start_{key}: corresponds to the first() value of the groups
    - end_{key}: corresponds to the last() value of the groups
    - value: corresponds to the last() - first() of the groups
    In addition to the `get_start_end_diff_args` items, `agg_diffs_df_of` also computes the columns for the `DEFAULT_DIFF_VARS` dict.
    ### Parameters:
    - query: string to be fed to the query method.
    - grp_by:    
        If provided, will be directly fed to grouby method. 
        Otherwise, assuming that query is a boolean column that served as the source of a perf and idx column using `perf_mask_and_idx_from_condition_mask`.
        In that case, `query = f"{query}_perf_mask` and `grp_by = f"{query}_perf_idx"`.
    """
    if not grp_by:
        grp_by = f"{query}_perf_idx"
        query = f"{query}_perf_mask"
    perfs_grps:DF_grp_by = vehicle_df.query(query).groupby(grp_by)
    period_diffs_df_dict = {}
    diff_cols = {**get_start_end_diff_args, **DEFAULT_DIFF_VARS}
    for source_col, dest_col in diff_cols.items():
        period_diffs_df_dict = {**get_start_end_diff(perfs_grps, source_col, dest_col), **period_diffs_df_dict}

    diffs_df = DF(period_diffs_df_dict)
    diffs_df = (
        diffs_df
        .assign(mean_date=diffs_df["start_date"] + diffs_df["duration"] / 2)
        .eval("mean_odo = start_odometer + distance / 2")
        .assign(
            soc_diff=diffs_df["soc_diff"].abs(),
            size=perfs_grps.size(),
            sec_duration=diffs_df["duration"].dt.total_seconds(),
        )
        .query("size > 1")
    )

    return diffs_df

def get_start_end_diff(grp: DF_grp_by, col_str: str, diff_col_name:str) -> dict[str, Series]:
    data_dict = {
        f"start_{col_str}": grp[col_str].first(),
        f"end_{col_str}": grp[col_str].last(),
        diff_col_name: grp[col_str].last() - grp[col_str].first()
    }

    return data_dict

