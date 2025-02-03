from logging import Logger, getLogger

from scipy.stats import linregress as lr
import numpy as np
from scipy.optimize import minimize

from core.pandas_utils import * 


logger = getLogger("core.stats_utils")

def filter_results_by_lines_bounds(results: DF, valid_soh_points: DF, logger: Logger=logger) -> DF:
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
    if results["soh"].isna().all():
        logger.debug("No SoH values to filter, column is all NaN returning as is.")
        return results
    max_intercept, max_slope = intercept_and_slope_from_points(valid_soh_points.xs("max", level=0, drop_level=True))
    min_intercept, min_slope = intercept_and_slope_from_points(valid_soh_points.xs("min", level=0, drop_level=True))
    results = (
        results
        .eval("raw_soh = soh")
        .eval(f"max_valid_soh = odometer * {max_slope:f} + {max_intercept:f}")
        .eval(f"min_valid_soh = odometer * {min_slope:f} + {min_intercept:f}")
        .eval(f"soh_is_valid = soh.between(min_valid_soh, max_valid_soh) & soh.between(0.5, 1.0)")
        .assign(soh = lambda df: np.where(df.soh_is_valid, df.soh, np.nan))
    )
    nb_rows_removed = results["soh_is_valid"].eq(False).sum()
    if results.shape[0]:
        rows_removed_pct = 100 * nb_rows_removed / results.shape[0]
        if nb_rows_removed == results.shape[0]:
            logger.warning(f"While filtering, all SoH results were set to NaN, check the valid SOH points.")
        else:
            logger.debug(f"Filtered SoH results, {nb_rows_removed}({rows_removed_pct:.2f}%) set to NaN.")
    else: 
        logger.warning("No SoH results to filter.")
    return results

def intercept_and_slope_from_points(points: DF) -> tuple[float, float]:
    slope = (points.at["B", "soh"] - points.at["A", "soh"]) / (points.at["B", "odometer"] - points.at["A", "odometer"])
    intercept = points.at["A", "soh"] - slope * points.at["A", "odometer"]
    return intercept, slope

def lr_params_as_series(df: DF, x: str, y: str) -> Series:
    df = df.dropna(subset=[x, y], how="any")
    s = Series(lr(df[x], df[y]), index=["slope", "intercept", "rvalue", "pvalue", "stderr"])
    s["r2"] = s["rvalue"] ** 2
    return s

def mask_out_outliers_by_interquartile_range(soh_values:Series) -> Series:
    q1, q3 = soh_values.quantile(0.05), soh_values.quantile(0.95)
    return soh_values.between(q1, q3, inclusive="both")

def force_monotonic_decrease(values:Series) -> Series:
    """
    Ajuste les valeurs de SoH pour garantir une décroissance tout en minimisant 
    l'écart avec les valeurs brutes.
    """
    values = values.copy()
    notna_values = values.dropna()
    nb_vals = notna_values.size
    # Fonction objectif : minimiser l'écart quadratique entre les valeurs ajustées et les valeurs brutes
    def objectif(adjusted_values:np.ndarray) -> float:
        return np.sum((adjusted_values - notna_values) ** 2)
   # Contraintes : SoH doit être non-croissant et l'écart hebdomadaire doit être inférieur à 0,1%
    constraints = [{'type': 'ineq', 'fun': lambda x, i=i: (x[i - 1] - x[i]) * 0.00001} for i in range(1, nb_vals)] 
    constraints += [{'type': 'ineq', 'fun': lambda x, i=i: 0.00005 * x[i - 1] - (x[i - 1] - x[i])} for i in range(1, nb_vals)]
    # Borne supérieure et inférieure (par exemple, entre 0 et 100)
    bounds = [(0, 100)] * nb_vals
    # Initialisation des valeurs ajustées (on commence par les valeurs brutes)
    # Résolution de l'optimisation
    results = minimize(objectif, notna_values, constraints=constraints, bounds=bounds, method="SLSQP")
    values[values.notna()] = results.x

    if results.success:
        return values
    else:
        raise ValueError("Optimisation failed:\n", results.message)

