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
from rich.traceback import install as install_rich_traceback

def main():
    vehicle_df = pd.read_parquet("data_cache/vehicles_raw_time_series/WBY71AW010FP87013.parquet").sort_index()
    # Preprocessing
    # Drop duplicates
    vehicle_df = vehicle_df.loc[vehicle_df.index.drop_duplicates()]
    vehicle_df["date"] = vehicle_df.index
    vehicle_df = vehicle_df.drop_duplicates("date") # For some reason, DF.index.drop_duplicates does not work buy DF.drop_duplicates works.
    # Status
    vehicle_df["status"] = vehicle_df["status"].astype("string")
    vehicle_df = process_time_series(vehicle_df)
    # Mode
    vehicle_df["mode"] = vehicle_df["mode"].astype("category")
    
    # self discharge perfs
    self_discharge_grps = vehicle_df.query("self_discharge_mask").groupby("self_discharge_period_idx")
    self_discharge_perfs_df = self_discharge_grps.apply(compute_period_perfs, "duration", "soc_diff", "secs_per_soc", include_groups=False) #, soc_trim_off_method=None)
    # charge perfs
    charge_perfs_grps = vehicle_df.query("in_charge_perf_mask").groupby("charge_period_idx")
    charging_perfs_df = charge_perfs_grps.apply(compute_period_perfs, "soc_diff", "duration", "soc_per_secs", include_groups=False) #, soc_trim_off_method=None)
    if not charging_perfs_df.empty:
        charging_perfs_df["soc_per_hour"] = charging_perfs_df["soc_per_secs"] * 3600
    # motion perfs
    motion_perfs_grps = vehicle_df.query("motion_perf_period_mask").groupby("motion_period_idx")
    motion_perfs_df = motion_perfs_grps.apply(compute_period_perfs, "dist", "soc_diff", "dist_per_soc", include_groups=False) #, soc_trim_off_method=None)

    print("self discharge:")
    print(self_discharge_perfs_df)
    
    print("charging_perfs_df:")
    print(charging_perfs_df)
    print(charging_perfs_df[["nb_points", "duration", "soc_diff", "soc_per_secs", "soc_per_hour"]].agg(["mean", "median", "min", "max", first_quart, third_quart]))
    # print(compute_perf_meta_data_df(charging_perfs_df, "soc_per_hour"))

    print("dist per soc perfs:")
    motion_perfs_df = motion_perfs_df.query("nb_points > 7")
    print(motion_perfs_df)
    print(motion_perfs_df[["nb_points", "duration", "soc_diff", "dist", "dist_per_soc"]].agg(["mean", "median", "min", "max", first_quart, third_quart]))
    print()
    # print(compute_perf_meta_data_df(motion_perfs_df, "dist_per_soc"))
    
    plt_perf_processing(vehicle_df, self_discharge_perfs_df, motion_perfs_df, charging_perfs_df)

def first_quart(s):
    return s.quantile(0.25)
def third_quart(s):
    return s.quantile(0.75)
    

def compute_perf_meta_data_df(perf_df: DF, perf_col: str) -> DF:
    if perf_df.empty:
        return DF()

    return perf_df.drop(columns=[perf_col]).corrwith(perf_df[perf_col]) 

def compute_period_perfs(vehicle_perf_period_df: DF, numerator:str, denumerator:str, perf_col:str, soc_trim_off_method:str="edge") -> dict:
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
        # "mode": vehicle_perf_period_df["mode"]
    }
    data_dict["duration"] = (data_dict["end_date"] - data_dict["start_date"]).total_seconds()
    data_dict["soc_diff"] = np.abs(vehicle_perf_period_df["batteryLevel"].max() - vehicle_perf_period_df["batteryLevel"].min())
    data_dict[perf_col] = data_dict[numerator] / data_dict[denumerator]
    series = pd.Series(data_dict)

    return series

MIN_SELF_DISCHARGE_SAMP_FREQ = 0.05
def process_time_series(vehicle_df: DF) -> DF:
    # standby mask
    vehicle_df["in_standby"] = vehicle_df["odometer"].diff().eq(0, fill_value=True)
    standby_period_idx = period_idx_of_mask(vehicle_df["in_standby"])
    # vehicle_df["in_standby"] &= vehicle_df["in_standby"].groupby(standby_period_idx).transform(lambda period: len(period) >= 100)
    # In motion mask
    vehicle_df["in_motion"] = ~vehicle_df["in_standby"]
    standby_period_idx = period_idx_of_mask(vehicle_df["in_motion"])
    # vehicle_df["in_motion"] &= vehicle_df["in_motion"].groupby(standby_period_idx).transform(lambda period: len(period) >= 100)
    # in charge mask
    vehicle_df["in_charge"] =  vehicle_df.eval("status == 'CHARGINGACTIVE' | status == 'CHARGINGENDED'")
    vehicle_df["in_charge_perf_mask"] = vehicle_df["in_charge"] & (vehicle_df["batteryLevel"].isna() | vehicle_df["batteryLevel"].between(20, 80, inclusive="neither"))
    vehicle_df["charge_period_idx"] = period_idx_of_mask(vehicle_df["in_charge_perf_mask"])
    vehicle_df["in_charge_perf_mask"] &= vehicle_df.groupby("charge_period_idx")["batteryLevel"].transform(perf_period_is_valid, min_samp_freq=None)
    # discharge mask
    vehicle_df["soc_decreasing"] = ~vehicle_df["in_charge"]
    # self discharge mask
    vehicle_df["self_discharge_mask"] = vehicle_df.eval("in_standby & soc_decreasing")
    vehicle_df["self_discharge_period_idx"] = period_idx_of_mask(vehicle_df["self_discharge_mask"])
    vehicle_df["self_discharge_mask"] &= vehicle_df.groupby("self_discharge_period_idx")["batteryLevel"].transform(perf_period_is_valid, min_samp_freq=None)
    # Spike less soc
    soc_cummin_in_decrease_periods = vehicle_df.groupby("self_discharge_period_idx")["batteryLevel"].transform("cummin")
    vehicle_df["soc"] = vehicle_df["batteryLevel"].mask(vehicle_df["self_discharge_mask"], soc_cummin_in_decrease_periods).ffill()
    # In motion dishcarge periods
    vehicle_df["motion_period_idx"] = period_idx_of_mask(vehicle_df["in_motion"])
    motion_discharge_is_valid = partial(perf_period_is_valid, min_duration=TD(seconds=2000), min_samp_freq=None)
    vehicle_df["motion_perf_period_mask"] = vehicle_df["in_motion"] & vehicle_df.groupby("motion_period_idx")["soc"].transform(motion_discharge_is_valid)

    return vehicle_df

def perf_period_is_valid(soc: pd.Series, min_duration:TD=TD(hours=1), min_soc_diff:int=3, min_samp_freq:float|None=0.05) -> bool:
    # There must be at least 3 soc drops
    # Two of which (first and last ones) will be ignored
    valid_soc_loss = abs(soc.max() - soc.min()) >= min_soc_diff and soc.nunique() >= 3
    duration: TD = soc.index.max() - soc.index.min()
    # There must be at least one hour of data
    valid_duration = duration >= min_duration
    # Check sampling frequency if minimum sampling frequency was specified
    secs = duration.total_seconds()
    valid_samp_freq = not min_samp_freq or (secs != 0 and (soc.count() / secs) >= min_samp_freq)

    return valid_soc_loss and valid_duration and valid_samp_freq

def period_idx_of_mask(mask: pd.Series) -> pd.Series:
    # shift MUST have the same "periods" parameter value as the diff "periods" parmeter  
    idx = mask.ne(mask.shift(), fill_value=False).cumsum()
    # idx.name = mask.name + "_period_idx"

    return idx

def plt_perf_processing(vehicle_df: DF, _: DF, in_motion_discharging_perfs_df: DF, charging_perfs_df: DF):
    # Plotting
    fig: Figure
    axs: list[Axes]
    fig, axs = plt.subplots(nrows=4, sharex=True, constrained_layout = True)
    # Odometer vars
    vehicle_df["odometer"].plot.line(ax=axs[0], marker="x")
    odo_max = vehicle_df["odometer"].max()
    odo_min = vehicle_df["odometer"].min()
    axs[0].fill_between(vehicle_df.index, odo_min, odo_max, where=vehicle_df["in_standby"], color="blue", alpha=0.6, label="in standby")
    axs[0].fill_between(vehicle_df.index, odo_min, odo_max, where=vehicle_df["in_motion"], color="green", alpha=0.6, label="in motion")
    if "internalTemperature" in vehicle_df.columns and "externalTemperature" in vehicle_df.columns:
        (vehicle_df["internalTemperature"] - vehicle_df["externalTemperature"]).plot.line(ax=axs[0].twinx(), label="temp diff", color="red")
    axs[0].set_title("odometer variables")
    axs[0].legend()
    # soc vars
    axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["soc_decreasing"], label="soc is decreasing", color="red")
    axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["in_charge"], label="in charge", color="blue")
    axs[1].fill_between(vehicle_df.index, 0, 100, where=vehicle_df["in_charge_perf_mask"], label="charging periods when soc is between 20, 80", color="green")
    vehicle_df["batteryLevel"].plot.line(ax=axs[1], label="batteryLevel", alpha=0.6, marker=".")
    vehicle_df["batteryLevel"].plot.line(ax=axs[1], label="batteryLevel", alpha=0.6, marker=".")
    vehicle_df["soc"].plot.line(ax=axs[1], label="soc",  alpha=0.6, marker=".")
    axs[1].set_title("SOC variables")
    # axs[1].legend()
    # dist per soc perfs
    plt_perf_period(in_motion_discharging_perfs_df, axs[2], "dist_per_soc", "motion discharge", label="motion discharge", color="green")
    # charging_perfs_df
    plt_perf_period(charging_perfs_df, axs[3], "soc_per_hour", "charging perfs", color="violet")
    
    plt.show()

def plt_perf_period(perf_periods_df: DF, ax:Axes, perf_col:str, title:str, **kwargs):
    ax.set_title(title)
    if perf_periods_df.empty:
        return
    # for _, row in perf_periods_df.iterrows():
    #     midpoint = row["start_date"] + (row["end_date"] - row["start_date"]) / 2
    #     text_x = date2num(midpoint)
    #     text_y = row[perf_col] / 2  # y-position for the text
    #     text = "\n".join([f"{key}: {val:.2f}" for key, val in row.to_dict().items() if not key in ["start_date", "end_date"]])
    #     ax.text(text_x, text_y, text, ha='center', va='center', fontsize=9, bbox=dict(facecolor='white', alpha=0.5))
    #     ax.axvspan(row["start_date"], row["end_date"], 0, row[perf_col] / perf_periods_df[perf_col].max(), **kwargs)
    # ax.set_ylim(0, perf_periods_df[perf_col].max())

    perf_periods_df = perf_periods_df.set_index("start_date", drop=False)
    perf_periods_df[perf_col].plot.line(ax=ax, label=perf_col)
    ax.legend()

if __name__ == "__main__":
    install_rich_traceback(width=130, extra_lines=0)
    try:
        main()
    except KeyboardInterrupt:
        print("exiting...")
