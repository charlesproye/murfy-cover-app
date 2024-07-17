import pandas as pd
from pandas import DataFrame as DF
import numpy as np
from rich import print
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.dates import date2num
from matplotlib.figure import Figure
from rich.traceback import install as install_rich_traceback

from watea.watea_constants import *
from watea.processed_watea_ts import iterate_over_processed_ts
from core.argparse_utils import parse_kwargs
import core.perf_agg_processing as perfs
from core.caching_utils import data_caching_wrapper
from core.pandas_utils import *
from core.plt_utils import plt_3d_df

def main():
    install_rich_traceback(extra_lines=0, width=130)
    kwargs = parse_kwargs(optional_args={"task":"compute_all", "force_update":False})
    if kwargs['task'] == "compute_all":
        for id, vehicle_df in iterate_over_processed_ts(query_str=kwargs.get("query_str", None)):
            perfs_dict = compute_perfs(vehicle_df, id, force_update=kwargs["force_update"])
    elif kwargs["task"] == "energy_distribution":
        fleet_data_df = get_fleet_wise_energy_distribution(**kwargs)
        plt_3d_distribution(fleet_data_df)

def plt_3d_distribution(fleet_df: DF):
    top_quantile = fleet_df["energy_added"].quantile(0.97)
    fleet_df = (
        fleet_df
        .assign(soc=fleet_df.index.get_level_values(2))
        .drop(columns=["duration"])
        .query("energy_added > 529 | sec_duration < 900")
    )

    plt_3d_df(fleet_df, "soc", "sec_duration", "energy_added", color="sec_duration")

def get_fleet_wise_energy_distribution(force_update=False, **kwargs) -> DF:
    return data_caching_wrapper(
        "",
        PATH_TO_FLEET_WISE_DISTRIBUTION,
        lambda _: compute_fleet_wise_energy_distribution(force_update=force_update, **kwargs),
        force_update=force_update
    )

def compute_fleet_wise_energy_distribution(**kwargs) -> DF:
        fleet_data_df: dict[str, DF] = {}
        for id, vehicle_df in iterate_over_processed_ts(query_str=kwargs.get("query_str", None)):
            distribution_df = compute_perfs(vehicle_df, id, force_update=kwargs["force_update"])["energy_distribution_per_soc"]
            fleet_data_df[id] = distribution_df
        fleet_data_df: DF = pd.concat(fleet_data_df, axis='index')
        fleet_data_df.index.names = ["id", "odometer", "temp", "soc"]
        fleet_data_df = (
            fleet_data_df
            .sort_index()
            .droplevel(0)
        )

        return fleet_data_df

def aggregate_fleet_wise_distribution_df(fleet_data_df: DF) -> DF:
        # Aggrgate
        fleet_data_df = (
             fleet_data_df
            .groupby(level=[1, 2, 3])
            .agg(["mean", "median", "count"])
            .query("('energy_added', 'count') >= 10")
        )

        print("count describe:", fleet_data_df.loc[:, ('energy_added', "count")].describe())
        print(fleet_data_df.loc[:, ("energy_added", ("median", "count"))])
        print(f"df memory usage: {total_MB_memory_usage(fleet_data_df):.2f}MB",)

        plt_energy_distribution(fleet_data_df, ("sec_duration", "median"), ("energy_added", "median"))

def plt_energy_distribution(distribution_df: DF, y_col:str, color_col:str):
    odo_uniques = distribution_df.index.get_level_values(0).unique()
    temps_value_counts = distribution_df.index.get_level_values(1).value_counts().sort_values()
    two_most_common_temps = temps_value_counts.iloc[-2:].index
    # Normalize temp values to [0, 1]
    max_col_val = distribution_df.loc[:, color_col].quantile(0.98)
    min_col_val = distribution_df.loc[:, color_col].quantile(0.02)
    norm = plt.Normalize(vmin=min_col_val, vmax=max_col_val)

    # Choose a colormap
    cmap = plt.cm.viridis  # Blue to red colormap

    fig: Figure
    axs: list[Axes]
    fig, axs = plt.subplots(nrows=len(two_most_common_temps), ncols=len(odo_uniques), sharey=True, sharex=True)

    for row_i, (row_axs, temp_value) in enumerate(zip(axs, two_most_common_temps)):
        temp_grp = distribution_df.xs(temp_value, level=1)
        for ax, odo_value in zip(row_axs, odo_uniques):
            odo_grp = temp_grp.xs(odo_value, level=0)
            energy_added_med = odo_grp.loc[:, y_col]
            normed_vals = odo_grp.loc[:, color_col].clip(min_col_val, max_col_val).sub(min_col_val).div(max_col_val - min_col_val)
            color = cmap(normed_vals)  
            print(normed_vals)
            print("=====")
            (
                energy_added_med
                # .mask(energy_added_med < energy_added_med.cummax(), np.nan)
                # .interpolate()
                .plot.line(
                    alpha=0.6,
                    linestyle="",
                    marker=".",
                    ax=ax,
                    color=color,
                )
            )
            if row_i == 0:
                ax.set_title(f"[{odo_value/1000:.0f}k, {((odo_value + ODOMETER_FLOOR_RANGE_FOR_ENERGY_DIST)/1000):.0f}k]")
    fig.suptitle("required energy to charge 0.5 soc based on current soc and temp")
    plt.show()


def compute_perfs(vehicle_df: DF, id:str, force_update=False) -> dict[str, DF]:
    return {
        # "discharge": compute_discharge_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC),
        "charge": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge", "energy_soh"),
        "charge_above_80": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge_above_80", "energy_soh_above_80"),
        "charge_bellow_80": compute_charging_perfs(vehicle_df, FORD_ETRANSIT_DEFAULT_KWH_PER_SOC, "in_charge_bellow_80", "energy_soh_bellow_80"),
        "energy_distribution_per_soc": charging_energy_distribution_of(id, vehicle_df, force_update=force_update)
    }

def compute_discharge_perfs(vehicle_df:DF, default_kwh_per_soc:float) -> DF:
    return (
        vehicle_df
        .pipe(perfs.agg_diffs_df_of, {"cum_energy": "energy_diff"}, "in_discharge",)
        .pipe(perfs.compute_soh_from_soc_and_energy_diff, "energy_diff", default_kwh_per_soc, "discharge_soh")
        .pipe(lambda df: df.assign(discharge_soh=df["discharge_soh"].replace(0, np.nan)))
        .eval("km_per_soc = distance / soc_diff")
        .pipe(lambda df: df.assign(km_per_soc=df["km_per_soc"].replace(0, np.nan)))
    ) 

def compute_charging_perfs(vehicle_df: DF, default_kwh_per_soc:float, in_charge_mask:str, energy_soh_name:str) -> DF:
    charge_perfs:DF = (
        vehicle_df
        .pipe(perfs.agg_diffs_df_of, {"cum_energy": "energy_added", "battery_range_km": "range_gained"}, in_charge_mask)
        .pipe(perfs.compute_soh_from_soc_and_energy_diff, "energy_added", default_kwh_per_soc, energy_soh_name)
        .pipe(lambda df: df.assign(energy_soh=df[energy_soh_name].replace(0, np.nan)))
        .eval("battery_range_added_soh = 100 * (range_gained / soc_diff) / @FORD_ETRANSIT_DEFAULT_KM_PER_SOC")
        .eval("sec_per_soc = sec_duration / soc_diff")
    )

    return charge_perfs

def charging_energy_distribution_of(id:str, vehicle_df: DF, force_update=False) -> DF:
    return data_caching_wrapper(
        id,
        PATH_TO_CHARGING_PERF_PER_SOC.format(id=id),
        lambda _: compute_charging_energy_distribution(vehicle_df),
        force_update,
    )

def compute_charging_energy_distribution(vehicle_df:DF) -> DF:
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
                vehicle_df["soc"].ffill() 
            ]
        )
        .pipe(lambda df: df.assign(energy_added=df["energy_added"].replace(0, np.nan)))
        .drop(columns=COLS_TO_DROP_FOR_ENERGY_DISTRIBUTION)
        .groupby(level=[0, 1, 3])
        .agg("mean")
        .sort_index()
        .eval("energy_added = energy_added * -1")
    )

if __name__ == '__main__':
    main()
