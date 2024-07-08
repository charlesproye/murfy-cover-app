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
    self_discharge_df = agg_diffs_df_of(
        vehicle_df,
        "in_self_discharge_perf_mask",
        "in_self_discharge_perf_idx",
        {cum_energy_spent_col: "energy_diff"}
    )
    self_discharge_df = compute_soh_from_soc_and_energy_diff(self_discharge_df, "energy_diff", stock_kwh_per_soc)
    self_discharge_df["secs_per_soc"] = self_discharge_df["duration"].dt.total_seconds() / self_discharge_df["soc_diff"]

    return self_discharge_df

def motion_perfs_df_of(vehicle_df: DF, stock_kwh_per_soc: float, cum_energy_spent_col:str="cum_energy_spent") -> DF:
    motion_perfs_df = agg_diffs_df_of(
        vehicle_df,
        "in_motion_perf_mask",
        "in_motion_perf_idx",
        {cum_energy_spent_col: "energy_diff"}
    )
    motion_perfs_df = compute_soh_from_soc_and_energy_diff(motion_perfs_df, "energy_diff", stock_kwh_per_soc, "motion_energy_soh")
    motion_perfs_df["dist_per_soc"] = motion_perfs_df["distance"] / motion_perfs_df["soc_diff"]

    return motion_perfs_df

def compute_soh_from_soc_and_energy_diff(perf_df: DF, kwh_energy_col: str, stock_kwh_per_soc, soh_col) -> DF:
    """
    ### Description:
    Computes the soh estimation based on a kWh diff, soc diff and stock kwh per soc ratio.
    Make sure the energy diff is in kwh.
    """
    print(perf_df.dtypes)
    return (
        perf_df
        .eval(f"kwh_per_soc = {kwh_energy_col} / soc_diff")
        .eval(f"{soh_col} = kwh_per_soc / {stock_kwh_per_soc}")
    )

def agg_diffs_df_of(vehicle_df: DF, query_str:str, groupby, get_start_end_diff_args: dict[str, str]) -> DF:
    perfs_grps:DF_grp_by = vehicle_df.query(query_str).groupby(groupby)
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

def get_start_end_diff(grp: DF_grp_by, col_str: str, diff_col_name:str=None, ) -> dict[str, Series]:
    data_dict = {
        f"start_{col_str}": grp[col_str].first(),
        f"end_{col_str}": grp[col_str].last(),
        diff_col_name: grp[col_str].last() - grp[col_str].first()
    }

    return data_dict

