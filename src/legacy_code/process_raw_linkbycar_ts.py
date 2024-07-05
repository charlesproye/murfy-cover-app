from functools import partial
from sys import argv
from datetime import timedelta as TD

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from pandas.api.typing import DataFrameGroupBy as DF_grp_by
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.dates import date2num

from fleet_vehicles_info import iterate_over_raw_time_series
from constants_variables import *

# Temporary
pd.set_option('future.no_silent_downcasting', True)

def main():
    if len(argv) == 1:
        iterator = iterate_over_raw_time_series()
    else:
        iterator = iterate_over_raw_time_series(query=f"is_electric & vin == '{argv[1]}'")

    for meta_data, vehicle_df in iterator:
        vehicle_df = process_raw_time_series(vehicle_df)
        perf_dfs_dict = perf_dfs_of(vehicle_df)
        plt_perf_computing(vehicle_df, meta_data, perf_dfs_dict)

def perf_dfs_of(vehicle_df: DF) -> dict[str, DF]:
    perf_dfs_dict = {
        # "charge": compute_perfs_df(vehicle_df, "in_charge_perf_mask", "charge_period_idx", "soc_per_secs = soc_diff / duration"),
        # "self_discharge": compute_perfs_df(vehicle_df, "self_discharge_mask", "self_discharge_period_idx", "secs_per_soc = duration / soc_diff"),
        "motion_discharge": compute_perfs_df(vehicle_df, "motion_discharge_mask", vehicle_df["date"].dt.floor("D"), "dist_per_soc = dist / soc_diff"),
    }
    perf_dfs_dict['motion_discharge'] = perf_dfs_dict['motion_discharge'].query("dist > 60")
    print(perf_dfs_dict['motion_discharge']["dist_per_soc"].describe())

    return perf_dfs_dict

def compute_perfs_df(vehicle_df: DF, mask_query:str, grp_by, eval_str: str) -> DF:
    masked_vehicle_df = vehicle_df.query(mask_query) if mask_query else vehicle_df
    perf_grps: DF_grp_by = masked_vehicle_df.groupby(grp_by)
    perf_data_dict: dict[str, Series] = {
        **get_start_end_diff(perf_grps, "date", "duration"),
        # **get_start_end_diff(perf_grps, "odometer", "dist"),
        "start_soc": perf_grps["soc"].first(),
        "end_soc": perf_grps["soc"].last(),
        "start_odometer": perf_grps["odometer"].first(),
        "end_odometer": perf_grps["odometer"].last(),
        "dist": perf_grps["odometer_diff"].agg("sum"),
        # Allows to calculate soc loss of non monotonic soc series.
        "soc_diff": abs(perf_grps["neg_soc_diff"].agg("sum")), 
        "len": perf_grps.size(),
    }
    # # Edge case where the first soc point is masked out
    # DONT REMOVE! 
    # For now it is still unclear if this is  helpfull or not, describe output of the evaluation don't show massive improvement on the test vin.
    # Let's first describe the output of the evaluation of all vehicles and then, if the comparaison between evaluation with and without this piece of code before (potentially)removing it.
    # edge_case_mask = perf_grps["soc"].first(numeric_only=False) != perf_grps["raw_soc"].first(numeric_only=False)
    # perf_data_dict["soc_diff"].loc[edge_case_mask] += 1
    # print(perf_data_dict["soc_diff"].loc[edge_case_mask])
    # Add charging mode if present in df
    if "charging_mode" in vehicle_df.columns:
        perf_data_dict["charging_mode"] = perf_grps["charging_mode"].agg(Series.mode)
    perf_data_dict["mean_date"] = perf_data_dict["start_date"] + perf_data_dict["duration"] / 2
    perf_data_dict["duration"] = perf_data_dict["duration"].dt.total_seconds()
    perf_df = DF(perf_data_dict)
    perf_df = perf_df.eval(eval_str)

    return perf_df

def get_start_end_diff(grp: DF_grp_by, col_str: str, diff_col_name:str=None, use_abs:bool=True) -> dict[str, Series]:
    data_dict = {
        f"start_{col_str}": grp[col_str].first(),
        f"end_{col_str}": grp[col_str].last(),
        diff_col_name: grp[col_str].last() - grp[col_str].first()
    }
    if use_abs:
        data_dict[diff_col_name] = abs(data_dict[diff_col_name])

    return data_dict

# TODO once the function is complete: compress into sub functions
def process_raw_time_series(vehicle_df: DF) -> DF:
    # remove duplicates
    vehicle_df = vehicle_df.loc[vehicle_df.index.drop_duplicates()]
    vehicle_df["date"] = vehicle_df.index
    vehicle_df = vehicle_df.drop_duplicates("date")
    # rename columns
    vehicle_df = vehicle_df.rename(columns={"batteryLevel": "raw_soc", "mode": "charging_mode"})
    # Drop nans
    vehicle_df = vehicle_df[vehicle_df["odometer"].notna() | vehicle_df["raw_soc"].notna()]
    # Infer "in_motion" from dodometer dir
    vehicle_df["in_motion"] = vehicle_df["odometer"].interpolate(method="time").diff().gt(0)
    # remove soc spikes
    # Save raw soc for plotting
    vehicle_df["soc"] = vehicle_df["raw_soc"]
    # Remove soc outliers by value 
    notna_soc_df = vehicle_df[["soc"]].dropna()
    notna_soc_df["next_soc"] = notna_soc_df["soc"].shift(-1)
    notna_soc_df["prev_soc"] = notna_soc_df["soc"].shift(1)
    notna_soc_df["is_spike"] = notna_soc_df.eval("next_soc == prev_soc & soc != next_soc")
    vehicle_df.loc[notna_soc_df.query("is_spike").index, "soc"] = np.nan
    # Remove soc outliers by direction
    vehicle_df["soc_dir"] = compute_dir_mask(vehicle_df["soc"])
    vehicle_df["soc_dir_idx"] = period_idx_of_mask(vehicle_df["soc_dir"], time_separation_thresh=None)
    soc_dir_grps = vehicle_df[vehicle_df["soc"].notna()].groupby("soc_dir_idx")
    soc_dir_grps_duration = soc_dir_grps["date"].last() - soc_dir_grps["date"].first()
    soc_diff_with_prev_grp = soc_dir_grps["soc"].last().shift() - soc_dir_grps["soc"].first()
    soc_diff_with_next_grp = soc_dir_grps["soc"].first().shift(-1) - soc_dir_grps["soc"].last()
    soc_dir_grps_is_spike = (soc_dir_grps_duration <= TD(minutes=20)) & soc_diff_with_prev_grp.abs().le(2, fill_value=True) & soc_diff_with_next_grp.abs().le(2, fill_value=True)
    vehicle_df["soc"] = vehicle_df.groupby("soc_dir_idx")["soc"].transform(lambda soc: np.nan if soc_dir_grps_is_spike.at[soc.name] else soc)
    # Compute soc_diff for later daily soc losses calculation from non monotonic soc series
    vehicle_df["soc_diff"] = vehicle_df["soc"].ffill().diff()
    vehicle_df["neg_soc_diff"] = vehicle_df["soc_diff"].mask(vehicle_df["soc_diff"] > 0, 0)
    # Compute odometer_diff 
    vehicle_df["odometer_diff"] = vehicle_df["odometer"].diff()
    # vehicle_df["neg_soc_diff"] = vehicle_df["soc_diff"].mask(vehicle_df["soc_diff"] > 0, 0)
    # print(vehicle_df["neg_soc_diff"].unique())
    # Infer "in_charge" mask from soc dir
    vehicle_df["in_charge"] = compute_dir_mask(vehicle_df["soc"])
    vehicle_df.loc[vehicle_df["soc"].isna(), "in_charge"] = np.nan
    vehicle_df["in_charge"] = vehicle_df["in_charge"].bfill()
    vehicle_df["charge_nb_points"] = vehicle_df.groupby(period_idx_of_mask(vehicle_df["in_charge"], time_separation_thresh=None))["soc"].transform(len)
    vehicle_df["in_charge"] |= vehicle_df["in_charge"].shift(-1, fill_value=False) & vehicle_df["charge_nb_points"].shift(-1, fill_value=0).eq(1)
    # Charge perf
    # Do not split far away charging points as some car only record the first and last charging points
    vehicle_df["charge_period_idx"] = period_idx_of_mask(vehicle_df["in_charge"], time_separation_thresh=None)
    vehicle_df["in_charge_perf_mask"] = vehicle_df["in_charge"] & vehicle_df.groupby("charge_period_idx")["soc"].transform(trim_off_mask_perf_period)
    vehicle_df["charge_period_idx"] = period_idx_of_mask(vehicle_df["in_charge_perf_mask"], time_separation_thresh=None)
    vehicle_df["in_charge_perf_mask"] &= vehicle_df.groupby("charge_period_idx")["soc"].transform(mask_charge_period)
    # self discharge mask
    vehicle_df["self_discharge_mask"] = vehicle_df.eval("~in_motion & ~in_charge")
    self_discharge_is_valid = partial(validate_perf_period, min_duration=TD(seconds=2000), min_samp_freq=None)
    vehicle_df["self_discharge_period_idx"] = period_idx_of_mask(vehicle_df["self_discharge_mask"])
    vehicle_df["self_discharge_mask"] &= vehicle_df.groupby("self_discharge_period_idx")["soc"].transform(trim_off_mask_perf_period)
    vehicle_df["self_discharge_period_idx"] = period_idx_of_mask(vehicle_df["self_discharge_mask"])
    vehicle_df["self_discharge_mask"] &= vehicle_df.groupby("self_discharge_period_idx")["soc"].transform(self_discharge_is_valid, min_samp_freq=None)
    # motion discharge agg by discharge cycle
    vehicle_df = on_event_motion_period_idx(vehicle_df)

    return vehicle_df

def always_recording_motion_period(vehicle_df: DF) -> DF:
    """
    ### Description:
    Computes motion perf period mask and index for vehicles that always record odometer values even when they are idel.
    """
    vehicle_df["motion_discharge_mask"] = vehicle_df.eval("in_motion & ~in_charge")
    vehicle_df["motion_discharge_period_idx"] = period_idx_of_mask(vehicle_df["motion_discharge_mask"])
    motion_discharge_grps = vehicle_df.groupby("motion_discharge_period_idx")["soc"]
    # vehicle_df["motion_discharge_mask"] &= motion_discharge_grps.transform(trim_off_mask_perf_period)
    vehicle_df["motion_discharge_mask"] &= motion_discharge_grps.transform(validate_perf_period, min_duration=None, min_samp_freq=None)

    return vehicle_df

def on_event_motion_period_idx(vehicle_df: DF) -> DF:
    """
    ### Description:
    Computes motion perf period mask and index for vehicles that record odometer values only during motion and set it to false during charging.
    """
    new_motion_period_mask = vehicle_df["date"].diff().ge(TD(minutes=20)) | vehicle_df["odometer"].isna()
    vehicle_df["motion_discharge_period_idx"] = new_motion_period_mask.cumsum()
    motion_discharge_grps = vehicle_df.groupby("motion_discharge_period_idx")
    vehicle_df["motion_discharge_mask"] = motion_discharge_grps["odometer"].transform(lambda odo: (odo.max() - odo.min()) > 35) & vehicle_df["odometer"].notna()

    return vehicle_df

# TODO switch from bool to nullable bool and leave plateau in between soc increase and decrease null instead of ffill/bfill
def compute_dir_mask(soc: Series) -> Series:
    """
    ### Description:
    Compute boolean mask from the diff of the series while handling nans.
    """
    notna_soc_diff = soc.dropna().diff()
    notna_soc_increasing = notna_soc_diff.gt(0, fill_value=False)
    notna_soc_decreasing = notna_soc_diff.lt(0, fill_value=False)
    soc_increasing_idx = notna_soc_diff[notna_soc_increasing].index
    soc_decreasing_idx = notna_soc_diff[notna_soc_decreasing].index
    soc_dir_mask = Series(np.repeat(np.nan, len(soc)), index=soc.index)
    soc_dir_mask.loc[soc_increasing_idx] = 1.0
    soc_dir_mask.loc[soc_decreasing_idx] = 0.0
    soc_dir_mask = soc_dir_mask.ffill().astype(bool)

    return soc_dir_mask

def mask_charge_period(soc: Series) -> bool|Series:
    in_between_20_80_mask:Series = soc.between(20, 80, inclusive="neither").mask(soc.isna(), np.nan).ffill(limit_area="insisde").replace(np.nan, False).astype(bool)
    in_between_20_80_soc = soc[in_between_20_80_mask]
    if validate_perf_period(in_between_20_80_soc, min_duration=False, min_samp_freq=None):
        # print("nb points:", len(in_between_20_80_soc), "start_date:", soc.index.min(), "end_date:", soc.index.max())
        return in_between_20_80_mask.mul(len(in_between_20_80_soc) > 2)
    full_period_passed = abs(soc.max() - soc.min()) >= 5 # perf_period_is_valid(in_between_20_80_soc, min_duration=False, min_samp_freq=False, check_nunique_soc=False)

    return full_period_passed and len(soc) > 2

def mask_off_trailing_soc(soc: Series) -> Series:
    ffilled_soc = soc.ffill()
    return ffilled_soc.ne(ffilled_soc.iat[-1]).shift(fill_value=True)

def mask_off_leading_soc(soc: Series) -> Series:
    bfilled_soc = soc.bfill()
    bfilled_first = bfilled_soc.iat[0]
    return bfilled_soc.ne(bfilled_first) #.shift(-1, fill_value=True)

def trim_off_mask_perf_period(soc: Series) -> Series:
    return mask_off_trailing_soc(soc) & mask_off_leading_soc(soc)

def validate_perf_period(soc: Series, min_duration:TD=TD(hours=1), min_soc_diff:int=3, min_samp_freq:float|None=0.05, check_nunique_soc:bool=True) -> bool:
    # There must be at least 3 soc plateaus
    # because first and last ones will be ignored
    valid_soc_loss = abs(soc.max() - soc.min()) >= min_soc_diff
    valid_nb_unique = not check_nunique_soc or (soc.nunique() > min_soc_diff and len(soc) > min_soc_diff)
    duration: TD = soc.index.max() - soc.index.min()
    # There must be at least one hour of data
    valid_duration = not min_duration or duration >= min_duration
    # Check sampling frequency if minimum sampling frequency was specified
    secs = duration.total_seconds()
    valid_samp_freq = not min_samp_freq or (secs != 0 and (soc.count() / secs) >= min_samp_freq)

    return valid_soc_loss and valid_duration and valid_samp_freq and valid_nb_unique

def period_idx_of_mask(mask: Series, period_shift:int=1, time_separation_thresh:TD=TD(hours=1)) -> Series:
    # shift MUST have the same "periods" parameter value as the diff "periods" parmeter
    mask_for_idx = mask.ne(mask.shift(period_shift), fill_value=False)
    # if time_separation_thresh is specified and there are data points that are too far away in time from each other we add a true value.
    # This will increase the index when applying cumsum and create a new period.
    if not time_separation_thresh is None:
        mask_for_idx |= mask.index.to_series().diff() >= time_separation_thresh
    idx = mask_for_idx.cumsum()

    return idx

def plt_perf_computing(vehicle_df: DF, meta_data: Series, perf_dfs_dict:dict[str, DF]):
    # plot of masks
    def plt_mask_on_ax(ax: Axes, num_col: str, mask: Series, color:str):
        ax.fill_between(vehicle_df.index, vehicle_df[num_col].min(), vehicle_df[num_col].max(), mask, alpha=0.5, color=color)
    fig, axs = plt.subplots(nrows=3, sharex=True, sharey=False, figsize=(24, 6))
    # odometer
    plt_mask_on_ax(axs[0], "odometer", vehicle_df["motion_discharge_mask"], "green")
    # plt_mask_on_ax(axs[0], "odometer", ~vehicle_df["in_motion"], "red")
    vehicle_df["odometer"].dropna().plot.line(marker=".", ax=axs[0], color="red", label="raw odometer")
    vehicle_df["odometer"].add(10).interpolate(method="time").plot.line(marker=".", ax=axs[0], color="blue", label="interpolated odometer")
    axs[0].legend()
    axs[0].set_title("ododmeter variables")
    # is charging
    plt_mask_on_ax(axs[1], "soc", vehicle_df["in_charge"], "green")
    plt_mask_on_ax(axs[1], "soc", ~vehicle_df["in_charge"], "red")
    vehicle_df["raw_soc"].dropna().plot.line(marker=".", color="red", ax=axs[1])
    vehicle_df["soc"].dropna().plot.line(marker=".", color="blue", ax=axs[1])
    axs[1].legend()
    axs[1].set_title("soc variables")
    # self discharge
    # plt_perf_period(perf_dfs_dict["self_discharge"], axs[2], "secs_per_soc", "self discharge perfs", color="green")
    # motion discharge
    plt_perf_period(perf_dfs_dict["motion_discharge"], axs[2], "dist_per_soc", "motion perfs", color="green", alpha=0.6)
    # charge
    # plt_perf_period(perf_dfs_dict["charge"], axs[4], "soc_per_secs", "charge perfs", color="green")

    fig.suptitle(f"{meta_data['manufacturer']} {meta_data['vin']}")
    plt.show()


text_visible = False
def plt_perf_period(perf_periods_df: DF, ax:Axes, perf_col:str, title:str, **kwargs):
    ax.set_title(title)
    if perf_periods_df.empty:
        return
    perf_periods_df = perf_periods_df[perf_periods_df[perf_col].ne(perf_periods_df[perf_col].max())]
    
    #text
    for _, row in perf_periods_df.iterrows():
        midpoint = row["start_date"] + (row["end_date"] - row["start_date"]) / 2
        text_x = date2num(midpoint)
        text_y = row[perf_col]  # y-position for the text
        text = "\n".join([f"{key}: {val:.2f}" if isinstance(val, float) else f"{key}: {val}" for key, val in row.to_dict().items() if key not in ["start_date", "end_date"]])
        ax.text(text_x, text_y, text, ha='center', va='center', fontsize=9, bbox=dict(facecolor='white', alpha=0.5), visible=False)
        

    def on_key(event):
        global text_visible
        if event.key == 't':
            text_visible = not text_visible
            for text_obj in ax.texts:
                text_obj.set_visible(text_visible)
            plt.draw()
    plt.gcf().canvas.mpl_connect('key_press_event', on_key)

    
    # ylim
    if len(perf_periods_df) > 1:
        y_min = perf_periods_df[perf_col].min()
        y_max = perf_periods_df[perf_col].max()
        perf_min_to_max_diff = abs(y_max - y_min)
        y_ax_marging = perf_min_to_max_diff * 0.05
        ax.set_ylim(y_min - y_ax_marging, y_max + y_ax_marging)
    ax.bar(perf_periods_df["mean_date"], perf_periods_df[perf_col], perf_periods_df["end_date"] - perf_periods_df["start_date"], label=perf_col, **kwargs)
    perf_periods_df = perf_periods_df.set_index("mean_date", drop=False)
    perf_periods_df[perf_col].plot.line(ax=ax, label=perf_col, marker=".")
    ax.legend()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[blue]exiting...")
