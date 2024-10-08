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
from core.config import *

def plt_3d_df(df: DF, x:str, y:str, z:str, color:str=None, opacity=0.4, save_path:str=None, colorscale='Viridis', size=3, symbol=None, width=1500, height=1000) -> Figure:
    fig = go.Figure(data=[go.Scatter3d(
        x=df[x],
        y=df[y],
        z=df[z],
        mode='markers',
        marker=dict(
            size=size,
            opacity=opacity,
            color=df[color] if not color is None else color,
            colorscale=colorscale,
            colorbar=dict(title=color),
            symbol=df[symbol] if not symbol is None else None,
        ),
        
    )])
    fig = basic_fig_update(fig, x, y, z, width, height)
    # fig.update_yaxes(type="log")

    if save_path:
        ensure_that_dirs_exist(save_path)
        fig.write_html(save_path)

    return fig
    
def basic_fig_update(fig: Figure, x:str, y:str, z:str, width=2000, height=1200) -> Figure:
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
        width=width,
        height=height,
    )
