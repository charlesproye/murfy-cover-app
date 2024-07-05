from functools import partial
from datetime import timedelta as TD
from datetime import datetime as DT
from typing import LiteralString

import pandas as pd
from pandas import DataFrame as DF
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.dates import date2num
from rich import print

def main():
    vehicle_df = pd.read_parquet("data_cache/vehicles_raw_time_series/LRWYGCFS6PC552861.parquet").sort_index()
    # Preprocessing
    # Drop duplicates
    vehicle_df = vehicle_df.loc[vehicle_df.index.drop_duplicates()]
    vehicle_df["date"] = vehicle_df.index
    vehicle_df = vehicle_df.drop_duplicates("date") # For some reason, DF.index.drop_duplicates does not work buy DF.drop_duplicates works.
    # Status
    vehicle_df["status"] = vehicle_df["status"].astype("string")
    vehicle_df["status"] = vehicle_df["status"].mask(vehicle_df["status"] == "False", np.nan).ffill()

    vehicle_df = process_time_series(vehicle_df)
    
    print("sec per soc perfs:")
    self_discharge_grps = vehicle_df.query("self_discharge_mask").groupby("self_discharge_period_idx")
    sec_per_soc_perfs_df = self_discharge_grps.apply(compute_period_perfs, "duration", "soc_diff", "secs_per_soc", include_groups=False)
    print("charging_perfs_df:")
    charge_perfs_grps = vehicle_df.query("in_charge_perf_mask").groupby("charge_period_idx")
    charging_perfs_df = charge_perfs_grps.apply(compute_period_perfs, "soc_diff", "duration", "soc_per_secs", include_groups=False)
    charging_perfs_df["soc_per_hour"] = charging_perfs_df["soc_per_secs"] * 3600
    print("dist per soc perfs:")
    motion_perfs_grps = vehicle_df.query("motion_perf_period_mask").groupby("motion_period_idx")
    motion_perfs_df = motion_perfs_grps.apply(compute_period_perfs, "dist", "soc_diff", "dist_per_soc", include_groups=False) #.unstack(1)
    print("sec per soc perfs:")
    print(sec_per_soc_perfs_df)
    print("charging_perfs_df:")
    print(charging_perfs_df)
    print("dist per soc perfs:")
    print(motion_perfs_df)
    
    plt_perf_processing(vehicle_df, sec_per_soc_perfs_df, motion_perfs_df, charging_perfs_df)

# Compute sec/soc perfs
def compute_period_perfs(vehicle_perf_period_df: pd.DataFrame, numerator:str, denumerator:str, perf_col:str, soc_trim_off_method:str="edge") -> dict:
    # Chop off the leading and trailing palteaus
    # The reason for ignoring them is that we don't know how close the int soc value is close to realiy.
    # E.g: The leading soc value is 70%. It might be that the battery just dropped to 70% or that it was at 70% for an hour and it will soon reach 69% percents.
    # By only consideing inbound soc plateaus we are sure that the soc drop durations are accurate
    if soc_trim_off_method == "edge":
        if vehicle_perf_period_df["soc"].nunique() < 3:
            raise ValueError("soc column has strcly less than 3 unique values, this would raise an error while trying ot trim off soc.")
        first_soc_val = vehicle_perf_period_df["soc"].iat[0]
        vehicle_perf_period_df = vehicle_perf_period_df.query("soc != @first_soc_val")
        last_soc_val = vehicle_perf_period_df["soc"].iat[-1]
        vehicle_perf_period_df = vehicle_perf_period_df[vehicle_perf_period_df["soc"] != last_soc_val]

    if vehicle_perf_period_df.empty:
        return pd.Series()
    data_dict = {
        "start_date": vehicle_perf_period_df.index.min(),
        "end_date": vehicle_perf_period_df.index.max(),
        "nb_points": len(vehicle_perf_period_df),
        "dist": vehicle_perf_period_df["odometer"].max() - vehicle_perf_period_df["odometer"].min(),
    }
    data_dict["duration"] = (data_dict["end_date"] - data_dict["start_date"]).total_seconds()
    data_dict["soc_diff"] = np.abs(vehicle_perf_period_df["usableBatteryLevel"].max() - vehicle_perf_period_df["usableBatteryLevel"].min())
    data_dict[perf_col] = data_dict[numerator] / data_dict[denumerator]
    series = pd.Series(data_dict)

    return series

MIN_SELF_DISCHARGE_SAMP_FREQ = 0.05
def process_time_series(vehicle_df: DF) -> DF:
    # standby mask
    vehicle_df["in_standby"] = vehicle_df["odometer"].diff().eq(0, fill_value=False)
    standby_period_idx = period_idx_of_mask(vehicle_df["in_standby"])
    vehicle_df["in_standby"] &= vehicle_df["in_standby"].groupby(standby_period_idx).transform(lambda period: len(period) >= 100)
    # In motion mask
    vehicle_df["in_motion"] = ~vehicle_df["in_standby"]
    standby_period_idx = period_idx_of_mask(vehicle_df["in_motion"])
    vehicle_df["in_motion"] &= vehicle_df["in_motion"].groupby(standby_period_idx).transform(lambda period: len(period) >= 100)
    # in charge mask
    vehicle_df["in_charge"] =  vehicle_df.eval("status == 'Charging' | status == 'Complete'")
    vehicle_df["in_charge_perf_mask"] = vehicle_df["in_charge"] & (vehicle_df["usableBatteryLevel"].isna() | vehicle_df["usableBatteryLevel"].between(20, 80, inclusive="neither"))
    vehicle_df["charge_period_idx"] = period_idx_of_mask(vehicle_df["in_charge_perf_mask"])
    vehicle_df["in_charge_perf_mask"] &= vehicle_df.groupby("charge_period_idx")["usableBatteryLevel"].transform(perf_period_is_valid)
    # discharge mask
    vehicle_df["soc_decreasing"] = ~vehicle_df["in_charge"]
    # self discharge mask
    vehicle_df["self_discharge_mask"] = vehicle_df.eval("in_standby & soc_decreasing")
    vehicle_df["self_discharge_period_idx"] = period_idx_of_mask(vehicle_df["self_discharge_mask"])
    vehicle_df["self_discharge_mask"] &= vehicle_df.groupby("self_discharge_period_idx")["usableBatteryLevel"].transform(perf_period_is_valid)
    # Spike less soc
    soc_cummin_in_decrease_periods = vehicle_df.groupby("self_discharge_period_idx")["usableBatteryLevel"].transform("cummin")
    vehicle_df["soc"] = vehicle_df["usableBatteryLevel"].mask(vehicle_df["self_discharge_mask"], soc_cummin_in_decrease_periods).ffill()
    # In motion dishcarge periods
    vehicle_df["motion_period_idx"] = period_idx_of_mask(vehicle_df["in_motion"])
    motion_discharge_is_valid = partial(perf_period_is_valid, min_duration=TD(seconds=2000))
    vehicle_df["motion_perf_period_mask"] = vehicle_df["in_motion"] & vehicle_df.groupby("motion_period_idx")["soc"].transform(motion_discharge_is_valid)

    return vehicle_df

def perf_period_is_valid(soc: pd.Series, min_duration:TD=TD(hours=1), min_soc_diff:int=3, min_samp_freq:float|None=0.05) -> bool:
    # There must be at least 3 soc drops
    # Two of which (first and last ones) will be ignored
    valid_soc_loss = abs(soc.max() - soc.min()) >= min_soc_diff
    duration: TD = soc.index.max() - soc.index.min()
    # There must be at least one hour of data
    valid_duration = duration >= min_duration
    # Check sampling frequency if minimum sampling frequency was specified
    secs = duration.total_seconds()
    valid_samp_freq = not min_samp_freq or (secs != 0 and (soc.count() / secs) >= min_samp_freq)

    return valid_soc_loss and valid_duration and valid_samp_freq

# def duration_from_index(series: pd.Series) -> int:
#     return (series.index.max() - series.index.min()).total_seconds()

def period_idx_of_mask(mask: pd.Series) -> pd.Series:
    # Shit MUST have the same "periods" parameter value as the diff "periods" parmeter  
    idx = mask.ne(mask.shift(), fill_value=False).cumsum()
    # idx.name = mask.name + "_period_idx"

    return idx

def plt_perf_processing(vehicle_df: DF, sec_per_soc_perfs_df: DF, in_motion_discharging_perfs_df: DF, charging_perfs_df: DF):
    # Plotting
    fig: Figure
    axs: list[Axes]
    fig, axs = plt.subplots(nrows=5, sharex=True, constrained_layout = True)
    # Odometer vars
    vehicle_df["odometer"].plot.line(ax=axs[0], marker="x")
    odo_max = vehicle_df["odometer"].max()
    odo_min = vehicle_df["odometer"].min()
    axs[0].fill_between(vehicle_df.index, odo_min, odo_max, where=vehicle_df["in_standby"], color="blue", alpha=0.6, label="in standby")
    axs[0].fill_between(vehicle_df.index, odo_min, odo_max, where=vehicle_df["in_motion"], color="green", alpha=0.6, label="in motion")
    (vehicle_df["internalTemperature"] - vehicle_df["externalTemperature"]).plot.line(ax=axs[0].twinx(), label="temp diff", color="red")
    axs[0].set_title("odometer variables")
    axs[0].legend()
    # soc vars
    vehicle_df["batteryLevel"].plot.line(ax=axs[1], label="batteryLevel", alpha=0.6, marker=".")
    vehicle_df["usableBatteryLevel"].plot.line(ax=axs[1], label="usableBatteryLevel", alpha=0.6, marker=".")
    vehicle_df["soc"].plot.line(ax=axs[1], label="soc",  alpha=0.6, marker=".")
    axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["soc_decreasing"], alpha=0.6, label="soc is decreasing")
    axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["in_charge_perf_mask"], alpha=0.6, label="charging periods when soc is between 20, 80")
    axs[1].set_title("SOC variables")
    axs[1].legend()
    # self discharge perfs
    plt_perf_period(sec_per_soc_perfs_df, axs[2], "secs_per_soc", "self-discharge", label="self-discharge", color="blue")
    # dist per soc perfs
    plt_perf_period(in_motion_discharging_perfs_df, axs[3], "dist_per_soc", "motion discharge", label="motion discharge", color="green")
    # charging_perfs_df
    plt_perf_period(charging_perfs_df, axs[4], "soc_per_hour", "charging perfs", color="violet")
    
    plt.show()

def plt_perf_period(perf_periods_df: DF, ax:Axes, perf_col:str, title:str, **kwargs):
    for _, row in perf_periods_df.iterrows():
        midpoint = row["start_date"] + (row["end_date"] - row["start_date"]) / 2
        text_x = date2num(midpoint)
        text_y = row[perf_col] / 2  # y-position for the text
        text = "\n".join([f"{key}: {val:.2f}" for key, val in row.to_dict().items() if not key in ["start_date", "end_date"]])
        ax.text(text_x, text_y, text, ha='center', va='center', fontsize=9, bbox=dict(facecolor='white', alpha=0.5))
        ax.axvspan(row["start_date"], row["end_date"], 0, row[perf_col] / perf_periods_df[perf_col].max(), **kwargs)
    ax.set_title(title)
    ax.set_ylim(0, perf_periods_df[perf_col].max())

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("exiting...")
