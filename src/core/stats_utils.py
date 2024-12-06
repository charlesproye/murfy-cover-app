from logging import Logger, getLogger

from scipy.stats import linregress as lr

from core.pandas_utils import *

logger = getLogger("core.stats_utils")

def filter_results(results: DF, valid_soh_points: DF, logger: Logger) -> DF:
    """
    ### Description:
    Filter results based on the valid SOH points.
    ### Arguments:
    results: DF
        The results to filter must contain the columns "soh" and "odometer".
    valid_soh_points: DF
        The valid SOH points.
        Must contain the columns "bound", "point", "soh" and "odometer".
        The must contain 4 points that define the slope bounds of minimum and maximum SOH ranges.
        View 
    """
    logger.debug("Filtering results.")
    max_intercept, max_slope = intercept_and_slope_from_points(valid_soh_points.xs("max", level=0, drop_level=True))
    min_intercept, min_slope = intercept_and_slope_from_points(valid_soh_points.xs("min", level=0, drop_level=True))
    filtered_results = (
        results
        .eval(f"max_valid_soh = odometer * {max_slope:f} + {max_intercept:f}")
        .eval(f"min_valid_soh = odometer * {min_slope:f} + {min_intercept:f}")
        .eval(f"soh_is_valid = soh <= max_valid_soh & soh >= min_valid_soh & soh > 0.5 & soh < 1.0")
        .query("soh_is_valid")
        .dropna(subset=["soh", "odometer"], how="any")
    )
    nb_rows_removed = results.shape[0] - filtered_results.shape[0]
    rows_removed_pct = 100*nb_rows_removed/results.shape[0]
    if nb_rows_removed == results.shape[0]:
        logger.warning(f"All results were removed, check the valid SOH points.")
    else:
        logger.debug(f"Filtered results, removed {nb_rows_removed}({rows_removed_pct:.2f}%) rows:\n{filtered_results}")
    return filtered_results

def intercept_and_slope_from_points(points: DF) -> tuple[float, float]:
    logger.debug(f"points:\n{points}")
    slope = (points.at["B", "soh"] - points.at["A", "soh"]) / (points.at["B", "odometer"] - points.at["A", "odometer"])
    intercept = points.at["A", "soh"] - slope * points.at["A", "odometer"]
    return intercept, slope

def lr_params_as_series(df: DF, x: str, y: str) -> Series:
    df = df.dropna(subset=[x, y], how="any")
    return Series(lr(df[x], df[y]), index=["slope", "intercept", "rvalue", "pvalue", "stderr"])

