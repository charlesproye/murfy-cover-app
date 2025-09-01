import plotly.express as px
from plotly.graph_objects import Figure
import pandas as pd
from pandas import DataFrame as DF
from plotly.graph_objects import Figure
import plotly.graph_objects as go
from scipy.optimize import curve_fit
from .pandas_utils import *
from .config import *

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


def trendline_apply(x, f):
    return eval(f)

def show_trendline(df, trendline, trendline_max, trendline_min, model, odometer_column_name, soh_column_name):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df[odometer_column_name],
        y=df[soh_column_name],
        mode='markers',
        marker_color='rgba(50, 182, 193, .9)',
        name='SoH'
    ))

    x_sorted = df[odometer_column_name].sort_values()
    fig.add_trace(go.Scatter(
        x=x_sorted,
        y=trendline_apply(x_sorted, trendline['trendline']),
        mode='lines',
        line=dict(color='red'),
        name='Fit'
    ))

    fig.add_trace(go.Scatter(
        x=x_sorted,
        y=trendline_apply(x_sorted, trendline_max['trendline']),
        mode='lines',
        line=dict(color='green'),
        name='Upper'
    ))

    fig.add_trace(go.Scatter(
        x=x_sorted,
        y=trendline_apply(x_sorted, trendline_min['trendline']),
        mode='lines',
        line=dict(color='green'),
        name='Lower'
    ))

    fig.update_layout(
        width=1000,
        height=600,
        xaxis_title='Odometer',
        yaxis_title='State of Health (SoH)',
        legend_title='Légende',
        title=f"version: {model}",
        template='plotly_white',
        xaxis=dict(range=[0, 150000]),  # Change selon l'échelle souhaitée pour l'odomètre
        yaxis=dict(range=[.75, 1.1])     # Change selon l'échelle souhaitée pour le SoH
    )

    return fig
