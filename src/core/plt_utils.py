from typing import Callable, Generator

import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from pandas.api.types import is_bool_dtype
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.dates import date2num
from matplotlib.figure import Figure
from rich import print
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from core.constant_variables import *

text_show_cid = 0
perf_df_idx = 0
perf_col_idx = 0

def plt_fleet(fleet_iterator: Callable[..., Generator[tuple[str, DF, dict[str, DF]], None, None]], plt_layout:dict, x_col:str="date", show=True, title=None) -> tuple[Figure, np.ndarray[Axes]]:
    fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, title)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    for id, vehicle_df, perfs_dict in fleet_iterator():
        fill_single_axs_for_single_vehicle(vehicle_df, perfs_dict, ts_cols, perfs_cols, axs, x_col)
    if show:
            plt.show()

    return fig, axs

def plt_single_vehicle(vehicle_df: DF, perfs_dict:dict[str, DF], plt_layout:dict, x_col:str="date", show=True, title=None) -> tuple[Figure, np.ndarray[Axes]]:
    fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, title)
    fill_single_axs_for_single_vehicle(vehicle_df, perfs_dict, ts_cols, perfs_cols, axs, x_col)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    if show:
        plt.show()

    return fig, axs

def setup_fig_axs_and_layouts(plt_layout:dict, title=None) -> tuple[Figure, np.ndarray[Axes], list, dict]:
    # setup
    ts_cols:list[str|list[str]] = plt_layout.get("vehicle_df", [])
    perfs_cols: dict[str, str|list[str]] = plt_layout.get("perfs_dict", {})
    nb_rows = len(ts_cols) + sum([len(perf_cols) for _, perf_cols in perfs_cols.items()])
    fig: Figure
    axs: np.ndarray[Axes]
    fig, axs = plt.subplots(nrows=nb_rows, sharex=True, squeeze=True)

    if title:
        fig.suptitle(title)

    return fig, axs, ts_cols, perfs_cols

def fill_single_axs_for_single_vehicle(vehicle_df: DF, perfs_dict:dict[str, DF], ts_cols:list, perfs_cols:dict, axs:np.ndarray[Axes], x_col:str="date"):
    # plt the time series
    fill_axs_with_df(axs, vehicle_df, ts_cols, x_col)
    # plt perfs
    axs_offset = len(ts_cols)
    for perf_name, perfs_cols in perfs_cols.items():
        fill_axs_with_df(axs[axs_offset:], perfs_dict[perf_name], perfs_cols, X_TIME_SERIES_COL_TO_X_PERIOD_COL[x_col])
        axs_offset += len(perfs_cols)

def fill_axs_with_df(axs:np.ndarray[Axes], df: DF, ts_cols:dict[str, str|list], x_col:str="date"):
    for ts_col, ax in zip(ts_cols, axs):
        if isinstance(ts_col, str) or isinstance(ts_col, dict): 
            fill_ax(ax, df, x_col, ts_col)
        if isinstance(ts_col, list): 
            for sub_ts_col in ts_col:
                if sub_ts_col == "twinx":
                    # ax.legend()
                    ax = ax.twinx()
                else:
                    fill_ax(ax, df, x_col, sub_ts_col)

def set_titles_and_legends(axs:np.ndarray[Axes], ts_cols:dict[str, str|list], perfs_cols:dict):
    for ts_col, ax in zip(ts_cols, axs):
        ax.set_title(ts_col)
        ax.legend()
    axs_offset = len(ts_cols)
    for _, perf_cols in perfs_cols.items():
        for perf_col, ax in zip(perf_cols, axs[axs_offset:]):
            ax.set_title(perf_col)
            ax.legend()
        axs_offset += len(perf_cols)

def fill_ax(ax: Axes, df:DF, x:str, y:str|dict, plt_kwargs:dict=DEFAULT_LINE_PLOT_KWARGS):
    if isinstance(y, dict):
        assert "y" in y, "Passed dict to plot Axes but there is no column 'y' in that dict."
        plt_kwargs = {key: val for key, val in y.items() if key != "y"}
        y = y["y"]
    if plt_kwargs.get("kind", "line") == "hlines":
        plt_kwargs = {key: val for key, val in plt_kwargs.items() if key != "kind"}
        xmin, xmax = ax.get_xlim()
        ax.hlines(y, xmin, xmax, **plt_kwargs)
        ax.set_xlim(xmin, xmax)
    elif is_bool_dtype(df[y]):
        ax_min, ax_max = ax.get_ylim()
        ax.fill_between(df.index, ax_min, ax_max, df[y].values, color=plt_kwargs.get("color", "green"), alpha=plt_kwargs.get("alpha", 0.6), label=y)
        ax.set_ylim(ax_min, ax_max)
    else:
        ax.plot(df[x], df[y], label=y, **plt_kwargs)

def plt_single_vehicle_sohs(vehicle_df:DF, perfs_dict:dict[str, DF], x_col:str="odometer", **kwargs) -> tuple[Figure, np.ndarray[Axes]]:
    fig = None
    fig, axs = plt.subplots(4, sharex=True)
    fill_axs_by_sohs(vehicle_df, perfs_dict, axs, x_col=x_col, **kwargs)
    if "outside_temp" in  vehicle_df.columns:
        for ax in axs:
            twin_ax = ax.twinx()    
            vehicle_df["outside_temp"].plot.line(ax=twin_ax, color="red", linestyle="--")
    fig.tight_layout()
    fig.legend()
    fig.suptitle(f"all sohs based on {x_col}")

    plt.show()

    return fig, axs

def fill_axs_by_sohs(vehicle_df:DF, perfs_dict:dict[str, DF], axs: list[Axes], x_col:str="odometer", x_col_periods="mean_odo", time_series_alpha=0.7, perf_alpha=0.7, plt_variance:bool=False):
    def plot_variance(ax:Axes, time_series:Series):
        var_ax = ax.twinx()
        var_series = time_series.sub(time_series.mean()).rolling("12h", center=True).var()
        var_ax.plot(vehicle_df[x_col], var_series, alpha=time_series_alpha - 0.25, color='red', label=f"{time_series.name} variance")
    # range soh
    if plt_variance:
        plot_variance(axs[0], vehicle_df["range_soh"])
    axs[0].plot(vehicle_df[x_col], vehicle_df["range_soh"], marker=".", alpha=time_series_alpha, label="range_soh")
    # last charge soh
    if "last_charge_soh" in vehicle_df.columns:
        if plt_variance:
            plot_variance(axs[1], vehicle_df["last_charge_soh"])
        axs[1].plot(vehicle_df[x_col], vehicle_df["last_charge_soh"], marker=".", alpha=time_series_alpha, label="last_charge_soh")
    # energy added
    axs[2].plot(
        perfs_dict["charging_perfs"][x_col_periods],
        perfs_dict["charging_perfs"]["energy_soh"],
        marker=".",
        alpha=perf_alpha,
    )
    axs[3].plot(
        perfs_dict["charging_perfs"][x_col_periods],
        perfs_dict["charging_perfs"]["battery_range_added_soh"],
        marker=".",
        alpha=perf_alpha,
    )

    axs[0].set_title("range_soh")
    axs[1].set_title("last_charge_soh")
    axs[2].set_title("energy_soh")
    axs[3].set_title("charge_range_soh")
    axs[2].relim()
    axs[3].relim()

def plt_perf_computing(vehicle_df: DF, perf_dfs_dict:dict[str, DF]):
    # plot of masks
    def plt_mask_on_ax(ax: Axes, num_col: str, mask: Series, color:str):
        ax.fill_between(vehicle_df.index, vehicle_df[num_col].min(), vehicle_df[num_col].max(), mask, alpha=0.5, color=color)
    fig, axs = plt.subplots(nrows=6, sharex=True, sharey=False, figsize=(24, 6))
    # odometer
    vehicle_df["power"].plot.line(marker=".", color="red", ax=axs[0], alpha=0.5)
    vehicle_df["charger_power"].plot.line(marker=".", color="blue", ax=axs[0], alpha=0.5)
    axs[0].legend()
    axs[0].set_title("power variables")
    # # odometer
    # plt_mask_on_ax(axs[0], "odometer", vehicle_df["motion_discharge_mask"], "green")
    # vehicle_df["odometer"].dropna().plot.line(marker=".", ax=axs[0], color="red", label="raw odometer")
    # axs[0].legend()
    # axs[0].set_title("ododmeter variables")
    # is charging
    vehicle_df["soc"].dropna().plot.line(marker=".", color="blue", ax=axs[1])
    axs[1].legend()
    axs[1].set_title("soc variables")
    # # power
    # vehicle_df["power"].plot.line(marker=".", color="red", ax=axs[2], alpha=0.5)
    # vehicle_df["charger_power"].plot.line(marker=".", color="blue", ax=axs[2], alpha=0.5)
    # axs[2].legend()
    # axs[2].set_title("power variables")
    # Energy added
    vehicle_df["charge_energy_added"].plot.line(marker=".", color="red", ax=axs[2], alpha=0.5)
    axs[2].legend()
    axs[2].set_title("charge_energy_added")
    # cum energy
    vehicle_df["cum_energy_spent"].plot.line(ax=axs[3], marker=".")
    vehicle_df["cum_charging_energy"].plot.line(ax=axs[3], marker=".")
    axs[3].legend()
    axs[3].set_title("cum energy variables")
    # range soh
    vehicle_df["range_soh"].dropna().plot.line(marker=".", color="blue", ax=axs[4], label="range soh")
    vehicle_df["smoothed_soh"].dropna().plot.line(marker=".", color="red", ax=axs[4], label="range soh")
    axs[4].legend()
    axs[4].set_title("range soh")
    # perf period
    plt_perf_period(perf_dfs_dict, "charging_perfs", "battery_range_added_soh", axs[5], color="green", alpha=0.6)

    # def on_key(event):
    #     global perf_col_idx
    #     global perf_df_idx
    #     # change perf df to plot
    #     if event.key == 'left' or event.key == 'right':
    #         perf_df_idx += 1 if event.key == "up" else -1
    #         perf_col_idx = 0
    #         fig.canvas.mpl_disconnect(text_show_cid)
    #         plt_perf_period(perf_dfs_dict, axs[4], color="green", alpha=0.6)
    #         plt.draw()
    #     # Change perf col to plot of perf df
    #     if event.key == 'up' or event.key == 'down':
    #         perf_col_idx += 1 if event.key == "up" else -1
    #         fig.canvas.mpl_disconnect(text_show_cid)
    #         plt_perf_period(perf_dfs_dict, axs[4], color="green", alpha=0.6)
    #         plt.draw()

    # fig.canvas.mpl_connect('key_press_event', on_key)
    plt.show()

def plt_time_series(vehicle_df: DF, cols:list[str]):
    fig, axs = plt.subplots()

def plt_time_series_plotly(df:DF, cols:list[str], save_to:str=None, show=True):
    df = df[cols]
    # Create a subplot figure with one row per column
    nb_rows = len(df.columns) // 2
    fig = make_subplots(rows=nb_rows, cols=2, shared_xaxes=True, subplot_titles=df.columns)
    # Add traces for each column with specified colors
    for i, column in enumerate(df.columns):
        fig.add_trace(go.Scatter(x=df.index, y=df[column], mode='lines', name=column), row=(i % nb_rows)+1, col=(i // nb_rows) + 1)
    # Add a range slider to the x-axis of the last subplot
    fig.update_xaxes(rangeslider_visible=True, row=len(df.columns), col=1)
    # Center the subplot titles
    for annotation in fig['layout']['annotations']:
        annotation['x'] = 0.5  # Center the subplot title
    # Update layout to make the plots bigger and centered
    fig.update_layout(
        height=800,  # Set the figure height
        width=1000,  # Set the figure width
        title_text="Line Charts of DataFrame Columns with Range Slider",
        title_x=0.5,  # Center the main title
        margin=dict(t=50, b=50, l=50, r=50),  # Set the margins
        annotations=[dict(
            x=0.5,
            y=-0.1,  # Position below the last subplot
            xref='paper',
            yref='paper',
            showarrow=False,
            text='Use the slider to zoom in/out on the x-axis'
        )]
    )
    if show:
        fig.show()
    if save_to:
        fig.write_html(save_to)

def plt_only_perfs(vehicle_df: DF, perfs_dict: dict[str, DF]):
    # plot of masks
    fig, axs = plt.subplots(nrows=3, sharex=True, sharey=False, figsize=(24, 6))
    # range soh
    vehicle_df["range_soh"].dropna().plot.line(marker=".", color="blue", ax=axs[0], label="range soh")
    vehicle_df["smoothed_soh"].dropna().plot.line(marker=".", color="red", ax=axs[0], label="range soh")
    axs[0].legend()
    axs[0].set_title("range soh")
    # charging perf energy added 
    plt_perf_period(perfs_dict, "charging_perfs", "energy_soh", axs[1], color="green", alpha=0.6)
    # charging perf energy added 
    plt_perf_period(perfs_dict, "charging_perfs", "battery_range_added_soh", axs[2], color="green", alpha=0.6)

    plt.show()

text_visible = False
def plt_perf_period(perf_periods_dfs_dict: dict[str, DF], perf_df_name:str, perf_col:str, ax:Axes, plt_bars=False, **kwargs):
    ax.set_title(f"{perf_df_name} ({perf_col})")

    perf_periods_df = perf_periods_dfs_dict[perf_df_name]
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

    global text_show_cid
    text_show_cid = plt.gcf().canvas.mpl_connect('key_press_event', on_key)

    # ylim
    if len(perf_periods_df) > 1:
        y_min = perf_periods_df[perf_col].min()
        y_max = perf_periods_df[perf_col].max()
        perf_min_to_max_diff = abs(y_max - y_min)
        y_ax_marging = perf_min_to_max_diff * 0.05
        ax.set_ylim(y_min - y_ax_marging, y_max + y_ax_marging)
        
    if plt_bars:
        ax.bar(perf_periods_df["mean_date"], perf_periods_df[perf_col], perf_periods_df["end_date"] - perf_periods_df["start_date"], label=perf_col, **kwargs)
    # perf_periods_df = perf_periods_df.set_index("mean_date", drop=False)
    perf_periods_df[perf_col].plot.line(x="mean_date", ax=ax, label=perf_col, marker=".")
    # ax.legend()

def plt_vehicles(vins: list[str], get_vehicle_df: Callable[[str], DF], x:str, y:str|list[str]):
    """
    ### Description:
    Plots one or multiple time series.
    """
    # Sanitize inputs
    y = [y] if not isinstance(y, list) else y
    vins = [vins] if not isinstance(vins, list) else vins
    # plotting
    fig, axs = plt.subplots(nrows= y)
    for vin, x_ax in zip(vins, axs):
        vehicle_df: DF = get_vehicle_df(vin)
        for y_col, ax in zip(y, x_ax):
            ax.line(x=vehicle_df[x], y=vehicle_df[y_col], label=y_col)
            ax.set_title()
            ax.legend()
    plt.show()

