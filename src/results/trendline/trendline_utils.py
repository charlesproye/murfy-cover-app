import json
import logging

import numpy as np
from scipy.optimize import curve_fit
from sqlalchemy.sql import text

from activation.config.settings import LOGGING_CONFIG
from core.numpy_utils import numpy_safe_eval
from core.sql_utils import get_sqlalchemy_engine
from core.stats_utils import log_function

logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def compute_trendline_upper(true, fit, window_size=50, interval=(5, 95)):
    """Compute the upper trendline bound.

    Args:
        true (numpy.array): True values.
        fit (numpy.array): Fitted values for the mean trendline.
        window_size (int, optional): Window size. Defaults to 50.

    Returns:
        numpy.array: Upper trendline values.
    """
    residuals = true - fit

    bounds = np.array(
        [
            np.percentile(
                residuals[
                    max(0, i - window_size) : min(len(residuals), i + window_size)
                ],
                interval,
            )
            for i in range(len(residuals))
        ]
    )

    return fit + bounds[:, 1]


def compute_trendline_lower(true, fit, window_size=50, distribution=1.96):
    """Compute the lower trendline bound.

    Args:
        true (numpy.array): True values.
        fit (numpy.array): Fitted values for the mean trendline.
        window_size (int, optional): Window size. Defaults to 50.

    Returns:
        numpy.array: Lower trendline values.
    """
    local_std = np.array(
        [
            np.std(true[max(0, i - window_size) : min(len(true), i + window_size)])
            for i in range(len(true))
        ]
    )
    smooth = np.linspace(local_std[0], local_std[-1], len(local_std))
    return fit - distribution * smooth


def fit_lower_bound(x, y_lower):
    offset = min(y_lower.max(), 1)

    def f(x, a, b):
        return offset + a * np.log1p(x / b)

    coef, _ = curve_fit(f, x, y_lower, maxfev=20000)
    return coef, offset


def build_trendline_expressions(coef_mean, x_sorted, y_lower, y_upper):
    """
    Builds trendline expressions.
    Upper bound starts at 1 and remains above mean/lower bound.
    """
    # Mean trendline
    mean_expr = {"trendline": f"1 - {abs(coef_mean[0])} * np.log1p(x/{coef_mean[1]})"}
    y_fit_mean = log_function(x_sorted, *coef_mean)

    # Upper bound
    delta = np.mean(y_upper - y_fit_mean)
    coef_upper_a = coef_mean[0] + delta
    if abs(coef_upper_a) > abs(coef_mean[0]):
        logger.info(
            "Trendline coefficient for upper trendline is greater than the mean trendline"
        )
        coef_upper_a = abs(coef_mean[0]) / 2
    coef_upper_b = coef_mean[1]
    upper_expr = {"trendline": f"1 - {abs(coef_upper_a)} * np.log1p(x/{coef_upper_b})"}

    # Lower bound
    coef_lower, offset = fit_lower_bound(x_sorted, y_lower)
    lower_expr = {
        "trendline": f"{offset} - {abs(coef_lower[0])} * np.log1p(x/{coef_lower[1]})"
    }
    return mean_expr, upper_expr, lower_expr


def compute_trendline_functions(
    x_sorted, y_sorted, distribution=1.96, interval=(5, 95)
):
    coef_mean, _ = curve_fit(
        log_function,
        x_sorted,
        y_sorted,
        maxfev=10000,
        bounds=([-np.inf, 10000], [np.inf, 100000]),
    )

    y_fit = log_function(x_sorted, *coef_mean)

    y_lower = compute_trendline_lower(y_sorted, y_fit, distribution=distribution)
    y_upper = compute_trendline_upper(
        y_sorted, y_fit, window_size=100, interval=interval
    )

    mean_expr, upper_expr, lower_expr = build_trendline_expressions(
        coef_mean, x_sorted, y_lower, y_upper
    )

    return mean_expr, upper_expr, lower_expr


def update_database_trendlines(
    model_id, mean, upper, lower, trendline_bib=True, oem_id=None
):
    if oem_id:
        query = text("""
            UPDATE oem
            SET trendline = :mean, trendline_min = :low, trendline_max = :high
            WHERE id = :id
        """)
        params = {
            "mean": json.dumps(mean),
            "low": json.dumps(lower),
            "high": json.dumps(upper),
            "id": oem_id,
        }
    else:
        query = text("""
            UPDATE vehicle_model
            SET trendline = :mean, trendline_min = :low,
                trendline_max = :high, trendline_bib = :bib
            WHERE id = :id
        """)
        params = {
            "mean": json.dumps(mean),
            "low": json.dumps(lower),
            "high": json.dumps(upper),
            "id": model_id,
            "bib": trendline_bib,
        }

    with get_sqlalchemy_engine().begin() as conn:
        conn.execute(query, params)


def clean_battery_data(df, odometer_column, soh_colum):
    df_clean = df.rename(columns={odometer_column: "odometer", soh_colum: "soh"}).copy()
    df_clean = df_clean[
        ~((df_clean["odometer"] < 20000) & (df_clean["soh"] < 0.95))
        & (df_clean["soh"] >= 0.8)
    ]
    df_clean = df_clean.dropna(subset=["soh", "odometer"])
    df_clean["soh"] = np.minimum(df_clean["soh"], 1)
    return df_clean


def prepare_data_for_fitting(df):
    x = np.append(df["odometer"].values, 0)
    y = np.append(df["soh"].values, 1)

    idx = np.argsort(x)
    return x[idx], y[idx]


def filter_data(
    df,
    col_odometer,
    col_vin,
    km_lower,
    km_upper,
    vin_total,
    nbr_under,
    nbr_upper,
):
    nb_total_vins = df[col_vin].nunique()
    nb_lower = df[df[col_odometer] <= km_lower][col_vin].nunique()
    nb_upper = df[df[col_odometer] >= km_upper][col_vin].nunique()

    if nb_total_vins >= vin_total and nb_lower >= nbr_under and nb_upper >= nbr_upper:
        return nb_total_vins
    print("Not enough vehicles to compute trendlines")


def filter_trendlines(trendline, trendline_max, trendline_min):
    value_at_160k = numpy_safe_eval(expression=trendline["trendline"], x=160_000)
    if value_at_160k >= 0.95:
        print(f"The trendline value at 160,000 km is {value_at_160k}")
        return False

    kms = np.arange(0, 200_001, 50_000)
    max_values = numpy_safe_eval(trendline_max["trendline"], x=kms)
    min_values = numpy_safe_eval(trendline_min["trendline"], x=kms)

    idx_150k = 150_000 // 50_000
    diff_start = max_values[0] - min_values[0]
    diff_150k = max_values[idx_150k] - min_values[idx_150k]

    return (diff_start - diff_150k) < 0.015
