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
from core.constant_variables import *

def plt_fleet(fleet_iterator: Callable[..., Generator[tuple[str, DF, dict[str, DF]], None, None]], plt_layout:dict, x_col:str="date", title=None, show=True) -> tuple[Figure, np.ndarray[Axes]]:
    fig, axs, ts_cols, perfs_cols = setup_fig_axs_and_layouts(plt_layout, title)
    set_titles_and_legends(axs, ts_cols, perfs_cols)
    for id, vehicle_df, perfs_dict in fleet_iterator():
        fill_single_axs_for_single_vehicle(vehicle_df, perfs_dict, ts_cols, perfs_cols, axs, x_col)
    if show:
            plt.show()

    return fig, axs

def plt_single_vehicle(vehicle_df: DF, perfs_dict:dict[str, DF], plt_layout:dict, x_col:str="date", title=None, show=True) -> tuple[Figure, np.ndarray[Axes]]:
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
        fill_axs_with_df(axs[axs_offset:], perfs_dict[perf_name], perfs_cols, X_TIME_SERIES_COL_TO_X_PERIOD_COL.get(x_col, x_col))
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
        # autosize=True,
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
