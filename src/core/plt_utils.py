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
from plotly.graph_objects import Figure

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
    fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, ts_fig, title)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    for id, vehicle_df, only_ts_perfs in fleet_iterator():
        fill_single_axs_for_single_vehicle(vehicle_df, only_ts_perfs, ts_cols, perfs_cols, axs, x_col)
    if plt_energy_dist:
        # This query is specific to watea remove once energy soh with dist as been moved to core 
        fleet_perfs["charging_points"] = fleet_perfs["charging_points"].query("energy_added > 300 & energy_added < 500 & sec_duration < 900 & temp < 35 & power < 4 & power > 1.5")
        plt_charge_energy_data(fleet_perfs["charging_points"], fleet_perfs["charge_energy_dist"], dist_fig)
    if show:
        plt.show()

    return fig, axs

def plt_single_vehicle(vehicle_df: DF, perfs_dict:dict[str, DF], plt_layout:dict, default_100_soh_dist: Series, x_col:str="date", title=None, show=True) -> tuple[Figure, np.ndarray[Axes]]:
    ts_fig = plt.figure(layout='constrained', figsize=(16, 8))
    plt_energy_dist = plt_layout.get("plt_energy_dist", False)
    if plt_energy_dist:
        ts_fig, dist_fig = ts_fig.subfigures(nrows=2, squeeze=True, height_ratios=[0.2, 0.8])

    ts_fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, ts_fig, title)
    fill_single_axs_for_single_vehicle(vehicle_df, perfs_dict, ts_cols, perfs_cols, axs, x_col)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    if plt_energy_dist:
        # This query is specific to watea remove once energy soh with dist as been moved to core 
        perfs_dict["charging_points"] = (
            perfs_dict["charging_points"]
            .query("energy_added > 300 & energy_added < 500 & sec_duration < 900 & temp < 35 & power < 4 & power > 1.5")
        )
        if perfs_dict["charge_energy_dist"].index.get_level_values(0).nunique()  and perfs_dict["charge_energy_dist"].index.get_level_values(1).nunique():
            plt_charge_energy_data(perfs_dict["charging_points"], perfs_dict["charge_energy_dist"], dist_fig, default_100_soh_dist=default_100_soh_dist)

    if show:
        plt.show()

    return ts_fig, axs

def plt_charge_energy_data(charging_points: DF, dists: Series,fig: Figure, scatter_kwargs=DEFAULT_CHARGING_POINTS_PLT_KWARGS, default_100_soh_dist:Series=None) -> tuple[Figure, np.ndarray[Axes]]:
    axs = axs_for_energy_dist(fig, dists)
    lvl_0_idxs: pd.Index = dists.index.get_level_values(0).unique().sort_values()
    lvl_1_idxs: pd.Index = dists.index.get_level_values(1).unique().sort_values()

    print(dists.xs(25.0, level=1).drop_duplicates())

    # print(dists.index.get_level_values(1).unique())
    # print(charging_points.index.get_level_values(1).unique())
    
    for lvl_1_idx in lvl_1_idxs:
        points_lvl_0_xs = charging_points.xs(lvl_1_idx, level=1)
        dist_lvl_0_xs = dists.xs(lvl_1_idx, level=1)
        for lvl_0_idx in dist_lvl_0_xs.index.get_level_values(0).unique().sort_values():
            points_lvl_1_xs = points_lvl_0_xs.xs(lvl_0_idx, level=0)
            dist_lvl_1_xs = dist_lvl_0_xs.xs(lvl_0_idx, level=0)
            ax_y_idx =  lvl_1_idxs.get_loc(lvl_1_idx)
            ax_x_idx =  lvl_0_idxs.get_loc(lvl_0_idx)
            ax: Axes = axs[ax_y_idx, ax_x_idx]
            sc = ax.scatter(x=points_lvl_1_xs.index, y=points_lvl_1_xs["energy_added"], c=points_lvl_1_xs["power"], cmap='autumn', **scatter_kwargs)
            dist_lvl_1_xs.plot(ax=ax, color="green")
            if not default_100_soh_dist is None and lvl_1_idx in default_100_soh_dist.index:
                default_100_soh_dist.xs(lvl_1_idx).plot.line(ax=ax, linestyle="--", color="violet")

    if len(lvl_0_idxs) > 1:
        row_axs = axs[:-1, -1] if len(lvl_1_idxs) > 1 else axs[:, -1]
        for row_i, ax in enumerate(row_axs):
            xs_val = dists.index.get_level_values(1).unique().sort_values()[row_i]
            lvl_1_charge_energy_dist_xs = dists.xs(xs_val, level=1)
            for x_val in lvl_1_charge_energy_dist_xs.index.get_level_values(0).unique().sort_values():
                lvl_1_charge_energy_dist_xs.xs(x_val, level=0).plot.line(ax=ax, label=x_val)
                if not default_100_soh_dist is None and xs_val in default_100_soh_dist.index:
                    default_100_soh_dist.xs(xs_val).plot.line(ax=ax, linestyle="--", color="violet")

    if len(lvl_1_idxs) > 1:
        col_axs = axs[-1, :-1] if len(lvl_0_idxs) > 1 else axs[-1]
        for col_i, ax in enumerate(col_axs):
            lvl1_xs_val = lvl_0_idxs[col_i]
            lvl_0_charge_energy_dist_xs = dists.xs(lvl1_xs_val, level=0)
            for y_val in  lvl_0_charge_energy_dist_xs.index.get_level_values(0).unique().sort_values():
                lvl_0_charge_energy_dist_xs.xs(y_val, level=0).plot.line(ax=ax, label=y_val)

    for ax, lvl_1_idx in zip(axs[-1], dists.index.get_level_values(0).unique().sort_values()):
        ax.set_xlabel(f"soc\n{(lvl_1_idx/1000):.0f}km")

    for ax, lvl_0_idx in zip(axs[:, 0], dists.index.get_level_values(1).unique().sort_values()):
        ax.set_ylabel(f"energy\n{lvl_0_idx}C°")
    cbar = fig.colorbar(sc, ax=axs.ravel().tolist(), shrink=0.95)
    cbar.set_label('power')

def axs_for_energy_dist(fig: Figure, df:DF) -> np.ndarray[Axes]:
    nunique_lvl_0 = df.index.get_level_values(0).nunique()
    nunique_lvl_1 = df.index.get_level_values(1).nunique()
    return fig.subplots(
        nrows=nunique_lvl_1 + (1 if nunique_lvl_1 > 1 else 0), 
        ncols=nunique_lvl_0 + (1 if nunique_lvl_0 > 1 else 0),
        sharex=True,
        sharey=True,
        squeeze=False,
    )


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
        if not perf_name in  (["charge_energy_dist", "charging_points"] if x_col == "odometer" else ["charge_energy_dist", "charging_points", "energy_soh"]):
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

def fill_ax(ax: Axes, data:DF|Series, x:str, y:str|dict, plt_kwargs:dict=DEFAULT_LINE_PLOT_KWARGS):
    
    if isinstance(y, dict):
        assert "y" in y, "Passed dict to plot Axes but there is no column 'y' in that dict."
        plt_kwargs = {key: val for key, val in y.items() if key != "y"}
        y = y["y"]
    if plt_kwargs.get("kind", "line") == "hlines":
        plt_kwargs = {key: val for key, val in plt_kwargs.items() if key != "kind"}
        xmin, xmax = ax.get_xlim()
        ax.hlines(y, xmin, xmax, **plt_kwargs)
        ax.set_xlim(xmin, xmax)
    plt_y = data[y] if isinstance(data, DF) else data
    plt_x = data[x] if isinstance(data, DF) else data.index
    if is_bool_dtype(plt_y):
        ax_min, ax_max = ax.get_ylim()
        ax.fill_between(data.index, ax_min, ax_max, plt_y.values, color=plt_kwargs.get("color", "green"), alpha=plt_kwargs.get("alpha", 0.6), label=y)
        ax.set_ylim(ax_min, ax_max)
    else:
        ax.plot(plt_x, plt_y, label=y, **plt_kwargs)

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

def plt_3d_df(df: DF, x:str, y:str, z:str, color:str, opacity=0.4, save_path:str=None, colorscale='Viridis', size=3, symbol=None) -> Figure:
    fig = go.Figure(data=[go.Scatter3d(
        x=df[x],
        y=df[y],
        z=df[z],
        mode='markers',
        marker=dict(
            size=size,
            opacity=opacity,
            color=df[color],
            colorscale=colorscale,
            colorbar=dict(title=color),
            symbol=df[symbol] if not symbol is None else None,
        ),
        
    )])
    fig = basic_fig_update(fig, x, y, z)
    # fig.update_yaxes(type="log")

    if save_path:
        ensure_that_dirs_exist(save_path)
        fig.write_html(save_path)

    # fig.show()

    return fig
    
def plot_2d_line(df: pd.DataFrame, x_column: str, y_column: str, line_group_column: str, color: str = None, color_scale: str = None):
    """
    Creates a 2D line plot using Plotly with optional color and color scale.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing the data.
        x_column (str): The column name for the x-axis.
        y_column (str): The column name for the y-axis.
        line_group_column (str): The column name for grouping the lines.
        color (str, optional): The column name to use for the line color. Default is None.
        color_scale (str, optional): The color scale to use. Default is None.

    Returns:
        plotly.graph_objs._figure.Figure: The generated Plotly figure.
    """
    if color:
        # If color is provided, use px.line with color_discrete_sequence
        fig = px.line(
            df,
            x=x_column,
            y=y_column,
            line_group=line_group_column,
            color=color,
            color_discrete_sequence=px.colors.qualitative.Plotly if not color_scale else getattr(px.colors.qualitative, color_scale)
        )
    else:
        # If no color is provided, create a line plot without coloring
        fig = px.line(
            df,
            x=x_column,
            y=y_column,
            line_group=line_group_column
        )
    
    # Update the layout (optional)
    fig.update_layout(
        title=f'2D Line Plot of {y_column} vs {x_column} Grouped by {line_group_column}',
        xaxis_title=x_column,
        yaxis_title=y_column,
        legend_title=line_group_column if not color else color
    )
    
    # Show the plot
    fig.show()

    
def basic_fig_update(fig: Figure, x:str, y:str, z:str) -> Figure:
    return fig.update_layout(
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
