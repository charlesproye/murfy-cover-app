import numpy as np
import pandas as pd
from pandas import DataFrame as DF
from pandas import Series
import plotly.express as px
from plotly.graph_objects import Figure
from plotly.graph_objects import Trace
import plotly.graph_objects as go

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


def plotly_axvfill(df:DF, x:str, mask_col:str, bottom:float=0, top:float=1, **kwargs) -> Trace:
    df = df.set_index(x, drop=False)
    mask = df[mask_col]
    mask_changes_index = mask.shift(fill_value=False).ne(mask)[mask].index
    repeated_values = np.repeat(mask_changes_index.values, 2)
    x = (
        pd.DataFrame(repeated_values.reshape(-1, 4))
        .assign(nan=pd.NA)
        .T
        .unstack()
        .reset_index(level=1, drop=False)
        .astype({"level_1": "float"})
        .rename(columns={"level_1": "y", 0:"x"})
        .eval(f"y = y // 2 * {top} + {bottom}")
    )
    print("done")

    return go.Scatter(
        x=x["x"].values,
        y=x["y"].values,
        fill="toself",
        mode="none",  # No lines, just fill
        **kwargs
    )

