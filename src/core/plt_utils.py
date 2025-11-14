import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pandas import DataFrame as DF
from pandas import Series
from plotly.graph_objects import Figure
from scipy.optimize import curve_fit

from core import numpy_utils


def plt_3d_df(
    df: DF,
    x: str,
    y: str,
    z: str,
    color: str | None = None,
    opacity=0.5,
    colorscale="Rainbow",
    size=3,
    width=1500,
    height=1000,
    hover_name=None,
    **kwargs,
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
            **kwargs,
        )
        .update_traces(marker={"line": {"width": 0}})
        .update_layout(
            scene={"camera": {"projection": {"type": "orthographic"}}},
        )
    )


def scatter_and_arrow_fig(
    df: DF,
    x0: str,
    x1: str,
    y0: str,
    y1: str,
    id_col: str | None = None,
    marker_size: int = 8,
    scatter_kwargs: dict | None = None,
    arrow_kwargs: dict | None = None,
) -> Figure:
    """Returns a Figure containing two scatter plots of x0/y0 and x1/y1 as well as arrows from 0 to 1 points."""
    if arrow_kwargs is None:
        arrow_kwargs = {}
    if scatter_kwargs is None:
        scatter_kwargs = {}
    x = df_cols_to_series(df, [x0, x1], id_col)
    y = df_cols_to_series(df, [y0, y1], id_col)
    return px.scatter(x=x.values, y=y.values).add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="markers+lines",
            marker={
                "symbol": "arrow",
                "color": "royalblue",
                "size": marker_size,
                "angleref": "previous",
                "standoff": marker_size / 2,
            },
        )
    )


def df_cols_to_series(df: DF, cols: list[str], id_col: str) -> Series:
    return (
        df.assign(empty_col=pd.NA)
        .loc[:, [*cols, id_col, "empty_col"]]
        .set_index(id_col, append=True)
        .T.unstack()
    )


def plot_log(df: pd.DataFrame, column: str) -> go.Figure:
    """Plot the log decreasing SoH for each diferent type in a column

    Args:
        df (pd.DataFrame): dataframe with a columns odometer and SoH
        column (str): column name to compare value
    """

    def log_function(x, a):
        return 1 + a * np.log1p(x / 1000)

    fig = go.Figure()
    # create color
    model_colors = {
        value: px.colors.qualitative.Plotly[i]
        for i, value in enumerate(df[column].unique())
    }

    for value in df[column].unique():
        df_model_temp = (
            df[df[column] == value].dropna(subset="soh").sort_values("odometer").copy()
        )
        # fit log function
        popt, _ = curve_fit(
            log_function, df_model_temp["odometer"], df_model_temp["soh"]
        )
        x_vals = np.linspace(0.1, df_model_temp.odometer.max(), 500)
        y_vals = log_function(x_vals, *popt)

        # Couleur unique pour le modèle
        color = model_colors[value]

        # Génération des valeurs ajustées
        fig.add_traces(
            go.Scatter(x=x_vals, y=y_vals, name=f"{value} trend", line={"color": color})
        )
    return fig


def show_trendline(
    df,
    trendline,
    trendline_max,
    trendline_min,
    model,
    odometer_column_name,
    soh_column_name,
):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df[odometer_column_name],
            y=df[soh_column_name],
            mode="markers",
            marker_color="rgba(50, 182, 193, .9)",
            name="SoH",
        )
    )

    x_sorted = df[odometer_column_name].sort_values()
    fig.add_trace(
        go.Scatter(
            x=x_sorted,
            y=numpy_utils.numpy_safe_eval(
                expression=trendline["trendline"], x=x_sorted
            ),
            mode="lines",
            line={"color": "red"},
            name="Fit",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x_sorted,
            y=numpy_utils.numpy_safe_eval(
                expression=trendline_max["trendline"], x=x_sorted
            ),
            mode="lines",
            line={"color": "green"},
            name="Upper",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x_sorted,
            y=numpy_utils.numpy_safe_eval(
                expression=trendline_min["trendline"], x=x_sorted
            ),
            mode="lines",
            line={"color": "green"},
            name="Lower",
        )
    )

    fig.update_layout(
        width=1000,
        height=600,
        xaxis_title="Odometer",
        yaxis_title="State of Health (SoH)",
        legend_title="Légende",
        title=f"version: {model}",
        template="plotly_white",
        xaxis={
            "range": [0, 150000]
        },  # Change selon l'échelle souhaitée pour l'odomètre
        yaxis={"range": [0.75, 1.1]},  # Change selon l'échelle souhaitée pour le SoH
    )

    return fig
