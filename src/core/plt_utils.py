import plotly.express as px
from plotly.graph_objects import Figure
import pandas as pd
from pandas import DataFrame as DF
import plotly.express as px
from plotly.graph_objects import Figure
import plotly.express as px
import plotly.graph_objects as go
from scipy.optimize import curve_fit
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
        **kwargs
    ) -> Figure:
    """Abstracts away some of the boiler plate code when calling px.scatter_3d."""
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
            **kwargs
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
    """Returns a Figure containing two scatter plots of x0/y0 and x1/y1 as well as arrows from 0 to 1 points."""
    x = df_cols_to_series(df, [x0, x1], id_col)
    y = df_cols_to_series(df, [y0, y1], id_col) 
    return (
        px.scatter(x=x.values, y=y.values)
        .add_trace(
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
            )
        )
    )


def df_cols_to_series(df: DF, cols:list[str], id_col:str) -> Series:
    return (
        df
        .assign(empty_col=pd.NA)
        .loc[:, cols + [id_col, "empty_col"]]
        .set_index(id_col, append=True)
        .T
        .unstack()
    )


def plot_log(df: pd.DataFrame, column:str) -> go.Figure:
    """Plot the log decreasing SoH for each diferent type in a column

    Args:
        df (pd.DataFrame): dataframe with a columns odometer and SoH
        column (str): column name to compare value
    """
    def log_function(x, a):
        return 1 + a * np.log1p(x/1000)
    fig = go.Figure()
    # create color
    model_colors = {value: px.colors.qualitative.Plotly[i] for i, value in enumerate(df[column].unique())}

    for value in df[column].unique():
        df_model_temp = df[df[column]==value].dropna(subset='soh').sort_values('odometer').copy()
        # fit log function
        popt, _ = curve_fit(log_function, df_model_temp['odometer'], df_model_temp['soh'])
        x_vals = np.linspace(0.1,  df_model_temp.odometer.max(), 500)
        y_vals = log_function(x_vals, *popt)

        # Couleur unique pour le modèle
        color = model_colors[value]

        # Génération des valeurs ajustées
        fig.add_traces(go.Scatter(x=x_vals, y=y_vals, name=f'{value} trend', line=dict(color=color)))
    return fig
