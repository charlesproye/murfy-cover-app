"""
## Description:
Plots the variables used in time series processing using matplotlib (because plotly is too slow).
"""
from sys import argv

import pandas as pd
from pandas import DataFrame as DF
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.axes import Axes

def main():
    TESLA_VIN = "LRWYGCFS6PC552861"
    vin = TESLA_VIN
    if len(argv) > 1:
        if argv[1] == "bmw":
            vin = "WBY71AW010FP87013"
        else:
            vin = argv[1]

    df = pd.read_parquet(f"data_cache/vehicles_raw_time_series/{vin}.parquet")
    df["date"] = df.index


def plt_perf_processing(vehicle_df: DF, sec_per_soc_perfs_df: DF, in_motion_discharging_perfs_df: DF, charging_perfs_df: DF):
    # Plotting
    fig: Figure
    axs: list[Axes]
    fig, axs = plt.subplots(nrows=3, sharex=True, constrained_layout = True)
    # Odometer vars
    vehicle_df["odometer"].plot.line(ax=axs[0], marker="x")
    odo_max = vehicle_df["odometer"].max()
    odo_min = vehicle_df["odometer"].min()
    # axs[0].fill_between(vehicle_df.index, odo_min, odo_max, where=vehicle_df["in_standby"], color="blue", alpha=0.6, label="in standby")
    # axs[0].fill_between(vehicle_df.index, odo_min, odo_max, where=vehicle_df["in_motion"], color="green", alpha=0.6, label="in motion")
    (vehicle_df["internalTemperature"] - vehicle_df["externalTemperature"]).plot.line(ax=axs[0].twinx(), label="temp diff", color="red")
    axs[0].set_title("odometer variables")
    axs[0].legend()
    # soc vars
    vehicle_df["batteryLevel"].plot.line(ax=axs[1], label="batteryLevel", alpha=0.6, marker=".")
    vehicle_df["usableBatteryLevel"].plot.line(ax=axs[1], label="usableBatteryLevel", alpha=0.6, marker=".")
    vehicle_df["soc"].plot.line(ax=axs[1], label="soc",  alpha=0.6, marker=".")
    # axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["soc_decreasing"], alpha=0.6, label="soc is decreasing")
    # axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["in_charge_perf_mask"], alpha=0.6, label="charging periods when soc is between 20, 80")
    axs[1].set_title("SOC variables")
    axs[1].legend()
    # status
    
    vehicle_df["status"].axs[2]
    
    plt.show()

