from pandas import DataFrame as DF
import plotly.express as px
from plotly.graph_objects import Figure
from plotly.graph_objects import Trace

from core.config import *

def plt_3d_df(
        df: DF,
        x:str,
        y:str,
        z:str,
        color:str=None,
        opacity=0.5,
        colorscale='Rainbow',
        size=3,
        width=1500,
        height=1000,
        hover_name=None,
    ) -> Figure:
    return (
        px.scatter_3d(
            df,
            x,
            y,
            z,
            color,
            opacity=opacity,
            width=width,
            height=height,
            hover_name=hover_name,
            size=[size] * len(df),
            color_continuous_scale=colorscale,
        )
        .update_traces(marker=dict(line=dict(width=0)))
        .update_layout(
            scene=dict(
                camera=dict(
                    projection=dict(
                        type='orthographic'
                    )
                )
            ),
        )
    )

import plotly.graph_objects as go

def plotly_axvfill(df, x, mask_col, **kwargs):
    """
    Returns a Plotly Scatter trace to mimic matplotlib's axvfill functionality.

    Parameters:
        df (pd.DataFrame): DataFrame containing data.
        x (str): Column name to be used as x-axis values.
        mask_col (str): Column name with boolean values indicating where to fill.
        **kwargs: Additional styling options for the Plotly Scatter trace (e.g., color).
    
    Returns:
        go.Scatter: A Plotly trace that fills vertical regions based on `mask_col`.
    """
    fill_regions = []  # Store the start and end points of fill regions
    fill_active = False
    start = None
    
    # Identify fill regions based on the mask column
    for i in range(len(df)):
        if df[mask_col].iloc[i] and not fill_active:  # Start of a fill region
            start = df[x].iloc[i]
            fill_active = True
        elif not df[mask_col].iloc[i] and fill_active:  # End of a fill region
            fill_regions.append((start, df[x].iloc[i]))
            fill_active = False
    
    # If still in a fill region by the end of the loop, close it
    if fill_active:
        fill_regions.append((start, df[x].iloc[-1]))
    
    # Create scatter traces to shade each region
    traces = []
    for start, end in fill_regions:
        traces.append(go.Scatter(
            x=[start, start, end, end],
            y=[0, 1, 1, 0],
            fill="toself",
            mode="none",  # No lines, just fill
            **kwargs
        ))

    return traces
