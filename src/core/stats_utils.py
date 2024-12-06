from scipy.stats import linregress as lr

from core.pandas_utils import *

def filter_results(results: DF, valid_soh_points: DF) -> DF:
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
    print(f"max_intercept: {max_intercept}, max_slope: {max_slope}")
    min_intercept, min_slope = intercept_and_slope_from_points(valid_soh_points.xs("min", level=0, drop_level=True))
    return (
        results
        .eval(f"max_valid_soh = odometer * {max_slope:f} + {max_intercept:f}")
        .eval(f"min_valid_soh = odometer * {min_slope:f} + {min_intercept:f}")
        .eval(f"soh_is_valid = soh <= max_valid_soh & soh >= min_valid_soh & soh > 0.5 & soh < 1.0")
        .pipe(debug_df, subset=["soh", "max_valid_soh", "min_valid_soh", "soh_is_valid"], logger=logger)
        .query("soh_is_valid")
        .dropna(subset=["soh", "odometer"], how="any")
    )

def intercept_and_slope_from_points(points: DF) -> tuple[float, float]:
    logger.debug(f"points:\n{points}")
    slope = (points.at["B", "soh"] - points.at["A", "soh"]) / (points.at["B", "odometer"] - points.at["A", "odometer"])
    intercept = points.at["A", "soh"] - slope * points.at["A", "odometer"]
    return intercept, slope

def lr_params_as_series(df: DF, x: str, y: str) -> Series:
    df = df.dropna(subset=[x, y], how="any")
    return Series(lr(df[x], df[y]), index=["slope", "intercept", "rvalue", "pvalue", "stderr"])

