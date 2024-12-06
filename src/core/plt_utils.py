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

def fig_change_with_scatter_and_arrows(df: DF, x:str, old_y:str, new_y:str, new_x:str=None, id_col:str=None, marker_size:int=8) -> Figure:
    arrow_df = (
        df
        .assign(empty_col=pd.NA)
        .loc[:, [x, id_col, old_y, new_y, "empty_col"]]
        .set_index([x, id_col], append=True)
        .T
        .unstack()
        .to_frame()
        .rename(columns={0: old_y})
        .reset_index()
    )

    return (
        px.scatter(
            arrow_df,
            x,
            old_y,
            color=id_col,
        )
        .add_trace(
            go.Scatter(
                x=arrow_df[x],
                y=arrow_df[old_y],
                mode="markers+lines",
                marker=dict(
                    symbol="arrow",
                    color="royalblue",
                    size=marker_size,
                    angleref="previous",
                    standoff=marker_size / 2,
                ),
            )
        )
    )

