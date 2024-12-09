import plotly.express as px
from plotly.graph_objects import Figure
import pandas as pd
from pandas import DataFrame as DF
import plotly.express as px
from plotly.graph_objects import Figure
import plotly.express as px
import plotly.graph_objects as go

from core.pandas_utils import *
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

def scatter_and_arrow_fig(df: DF, x0:str, x1:str, y0:str, y1:str, id_col:str=None, marker_size:int=8, scatter_kwargs:dict={}, arrow_kwargs:dict={}) -> Figure:
    x = df_cols_to_series(df, [x0, x1], id_col)
    y = df_cols_to_series(df, [y0, y1], id_col) 
    display(x)
    display(y)
    fig = px.scatter(x=x.values, y=y.values)
    fig = fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="markers+lines",
                marker=dict(
                    symbol="arrow",
                    color="royalblue",
                    size=marker_size,
                    angleref="previous",
                    standoff=marker_size / 2,
                ),
                **arrow_kwargs,
            )
        )
    return fig


def df_cols_to_series(df: DF, cols:list[str], id_col:str) -> Series:
    return (
        df
        .assign(empty_col=pd.NA)
        .loc[:, cols + [id_col, "empty_col"]]
        .set_index(id_col, append=True)
        .T
        .unstack()
    )

