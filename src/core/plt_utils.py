"""
This module implements the plotting functionalities to visualize the ENTIERTY of the data pipeline.
Warning: Spaghetti code until we switch to OOP paradigm, maybe we should also use seaborn?.
"""
# While the code does not need to be as clean as the rest, it is crucial to see what is happening under the hood. 
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
import plotly.express as px

from core.caching_utils import ensure_that_dirs_exist
from core.constants import *

def plt_fleet(
        fleet_iterator: Callable[..., Generator[tuple[str, DF, dict[str, DF]], None, None]],
        plt_layout:dict,
        fleet_perfs: dict[str, DF|Series],
        x_col:str="date",
        title=None,
        show=True
    ) -> tuple[Figure, np.ndarray[Axes]]:
    ts_fig = plt.figure(layout='constrained', figsize=(16, 8))
    plt_energy_dist = plt_layout.get("plt_energy_dist", False)
    if plt_energy_dist:
        ts_fig, dist_fig = ts_fig.subfigures(nrows=2, squeeze=True, height_ratios=[0.2, 0.8])
    # only_ts_perfs = {perf_name: perf for perf_name, perf in perfs_dict if not perf_name in ["charge_energy_distribution", "charge_energy_points"] }
    fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, ts_fig, title)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    for id, vehicle_df, only_ts_perfs in fleet_iterator():
        fill_single_axs_for_single_vehicle(vehicle_df, only_ts_perfs, ts_cols, perfs_cols, axs, x_col)
    print(plt_energy_dist)
    if plt_energy_dist:
        # This query is specific to watea remove once energy soh with dist as been moved to core 
        fleet_perfs["charge_energy_points"] = fleet_perfs["charge_energy_points"].query("energy_added > 300 & energy_added < 500 & sec_duration < 900 & temp < 35 & power < 4 & power > 1.5")
        dist_axs = dist_fig.subplots(
            nrows=fleet_perfs["charge_energy_dist"].index.get_level_values(0).nunique(), 
            ncols=fleet_perfs["charge_energy_dist"].index.get_level_values(1).nunique(),
            sharex=True,
            sharey=True,
        )
        plt_charge_energy_data(fleet_perfs["charge_energy_points"], fleet_perfs["charge_energy_dist"], dist_axs)
    if show:
            plt.show()

    return fig, axs

def plt_single_vehicle(vehicle_df: DF, perfs_dict:dict[str, DF], plt_layout:dict, x_col:str="date", title=None, show=True) -> tuple[Figure, np.ndarray[Axes]]:
    fig = plt.figure(layout='constrained', figsize=(16, 8))
    fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, fig, title)
    fill_single_axs_for_single_vehicle(vehicle_df, perfs_dict, ts_cols, perfs_cols, axs, x_col)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    if show:
        plt.show()

    return fig, axs

def plt_charge_energy_data(charge_energy_points_df: DF,  charge_energy_dist_df: Series, axs: np.ndarray[Axes], scatter_kwargs=DEFAULT_CHARGE_ENERGY_POINTS_PLT_KWARGS) -> tuple[Figure, np.ndarray[Axes]]:
    # If we ever use more than 3 levels for indexing charge energy data (if we use different models in the same data structures for example) switch to a recursive implementation
    for lvl_0_idx, lvl_0_axs in zip(charge_energy_dist_df.index.get_level_values(0).unique(), axs):
        points_lvl_0_xs = charge_energy_points_df.xs(lvl_0_idx, 0)
        dist_lvl_0_xs = charge_energy_dist_df.xs(lvl_0_idx, 0)
        for lvl_1_idx, ax in zip(dist_lvl_0_xs.index.get_level_values(0).unique(), lvl_0_axs):
            points_lvl_1_xs = points_lvl_0_xs.xs(lvl_1_idx, 0)
            dist_lvl_1_xs = dist_lvl_0_xs.xs(lvl_1_idx, 0)
            ax: Axes
            ax.scatter(x=points_lvl_1_xs.index, y=points_lvl_1_xs["energy_added"], **scatter_kwargs)
            ax.plot(dist_lvl_1_xs.index, dist_lvl_1_xs.values, color="green")

def setup_fig_axs_and_layouts(plt_layout:dict, fig: Figure, title=None,) -> tuple[Figure, np.ndarray[Axes], list, dict]:
    # setup
    ts_cols:list[str|list[str]] = plt_layout.get("vehicle_df", [])
    perfs_cols: dict[str, str|list[str]] = plt_layout.get("perfs_dict", {})
    nb_rows = len(ts_cols) + sum([len(perf_cols) for _, perf_cols in perfs_cols.items()])
    fig: Figure
    axs = fig.subplots(nrows=nb_rows, sharex=True, squeeze=False)
    axs: np.ndarray[Axes] = axs[:, 0]

    if title:
        fig.suptitle(title)

    return fig, axs, ts_cols, perfs_cols

def fill_single_axs_for_single_vehicle(vehicle_df: DF, perfs_dict:dict[str, DF], ts_cols:list, perfs_cols:dict, axs:np.ndarray[Axes], x_col:str="date"):
    # plt the time series
    fill_axs_with_df(axs, vehicle_df, ts_cols, x_col)
    # plt perfs
    axs_offset = len(ts_cols)
    for perf_name, perfs_cols in perfs_cols.items():
        if not perf_name in  (["charge_energy_distribution", "charge_energy_points"] if x_col == "odometer" else ["charge_energy_distribution", "charge_energy_points", "energy_soh"]):
            fill_axs_with_df(axs[axs_offset:], perfs_dict[perf_name], perfs_cols, X_TIME_SERIES_COL_TO_X_PERIOD_COL.get(x_col, x_col))
            axs_offset += len(perfs_cols)

def fill_axs_with_df(axs:np.ndarray[Axes], df: DF, ts_cols:dict[str, str|list], x_col:str="date"):
    for ts_col, ax in zip(ts_cols, axs):
        if isinstance(ts_col, str) or isinstance(ts_col, dict): 
            fill_ax(ax, df, x_col, ts_col)
        if isinstance(ts_col, list): 
            for sub_ts_col in ts_col:
                if sub_ts_col == "twinx":
                    ax = ax.twinx()
                else:
                    fill_ax(ax, df, x_col, sub_ts_col)

def set_titles_and_legends(axs:np.ndarray[Axes], ts_cols:dict[str, str|list], perfs_cols:dict,):
    for ts_col, ax in zip(ts_cols, axs):
        if isinstance(ts_col, str):
            ax.set_title(ts_col)
        if isinstance(ts_col, dict) and "y" in ts_col: 
            ax.set_title(ts_col["y"])
        ax.legend()
    axs_offset = len(ts_cols)
    for _, perf_cols in perfs_cols.items():
        for perf_col, ax in zip(perf_cols, axs[axs_offset:]):
            if isinstance(perf_col, dict) and "y" in perf_col: 
                ax.set_title(perf_col["y"])
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

def plt_3d_df(df: DF, x:str, y:str, z:str, color:str, opacity=0.4, save_path:str=None, size=3):
    fig = go.Figure(data=[go.Scatter3d(
        x=df[x],
        y=df[y],
        z=df[z],
        mode='markers',
        marker=dict(
            size=size,
            opacity=opacity,
            color=df[color],
            colorscale='Viridis',
            colorbar=dict(title=color),

        )
    )])
    fig.update_layout(
        margin=dict(l=0, r=0, b=0, t=0),
        scene=dict(
            xaxis=dict(title=x),
            yaxis=dict(title=y),
            zaxis=dict(title=z),
            camera=dict(
                projection=dict(
                    type='orthographic'
                )
            )
        ),
        width=2000,  # Adjust width as needed
        height=1200   # Adjust height as needed
    )
    if save_path:
        ensure_that_dirs_exist(save_path)
        fig.write_html(save_path)

    fig.show()


# text_show_cid = 0
# perf_df_idx = 0
# perf_col_idx = 0
# text_visible = False
    # def on_key(event):
    #     global text_visible
    #     if event.key == 't':
    #         text_visible = not text_visible
    #         for text_obj in ax.texts:
    #             text_obj.set_visible(text_visible)
    #         plt.draw()

    # global text_show_cid
    # text_show_cid = plt.gcf().canvas.mpl_connect('key_press_event', on_key)
