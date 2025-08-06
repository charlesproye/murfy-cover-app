import warnings
from logging import Logger, getLogger

import numpy as np
from scipy.optimize import minimize
from scipy.stats import linregress as lr

from .pandas_utils import * 
# from transform.raw_results.get_tesla_soh_readouts import aviloo_readouts


logger = getLogger("core.stats_utils")

def filter_results_by_lines_bounds(results: DF, valid_soh_points: DF, logger: Logger=logger) -> DF:
    """
    ### Description:
    Filter results based on the valid SoH points.
    ### Arguments:
    results: DF
        The results to filter must contain the columns "soh" and "odometer".
    valid_soh_points: DF
        The valid SoH points.
        Must contain the columns "bound", "point", "soh" and "odometer".
        The must contain 4 points that define the slope bounds of minimum and maximum SOH ranges.
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
            logger.debug(f"Filtered SoH results out of bounds defined in processed_results.config, {nb_rows_removed}({rows_removed_pct:.2f}%) set to NaN.")
    else: 
        logger.warning("No SoH results to filter.")
    return results

def intercept_and_slope_from_points(points: DF) -> tuple[float, float]:
    slope = (points.at["B", "soh"] - points.at["A", "soh"]) / (points.at["B", "odometer"] - points.at["A", "odometer"])
    intercept = points.at["A", "soh"] - slope * points.at["A", "odometer"]
    return intercept, slope

def lr_params_as_series(df: DF, x: str, y: str) -> Series:
    """
    ### Description:
    Warp around scipy.stats.linear_regression to input and output pandas objects instead of np.ndarrays.  
    Additionallym computes the R2 which is not returned by default.  
    ### Returns:
    scipy.stats.linear_regression tuple output in a Series with the tuple value names as index.  
    """
    df = df.dropna(subset=[x, y], how="any")
    if df.empty or df[x].eq(df[x].iat[0]).all():
        return Series(data=[np.nan] * 6, index=["slope", "intercept", "rvalue", "pvalue", "stderr", "r2"])
    s = Series(lr(df[x], df[y]), index=["slope", "intercept", "rvalue", "pvalue", "stderr"])
    s["r2"] = s["rvalue"] ** 2
    return s

def mask_out_outliers_by_interquartile_range(soh_values:Series) -> Series:
    """Returns a mask that is False for values outside of the [5%, 95%] quantiles interval."""
    valid_float_soh_values = soh_values.dropna()
    q1, q3 = valid_float_soh_values.quantile(0.05), valid_float_soh_values.quantile(0.95)
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
    values.loc[values.notna()] = results.x.astype("float32")

    if results.success:
        return values
    else:
        raise ValueError("Optimisation failed:\n", results.message)


def evaluate_soh_estimations(results:DF, soh_cols:list[str]) -> DF:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="invalid value encountered in subtract")
        # This is an ugly pd.concat call but it's the first working I found :-)
        return pd.concat(
            {soh_col: evaluate_single_soh_estimation(results, soh_col) for soh_col in soh_cols},
            axis="columns"
        ).T

def evaluate_single_soh_estimation(results:DF, soh_col:DF) -> DF:
    from core.config import BASE_SLOPE
    lr_params:DF = (
        results
        .groupby("vin", observed=True, as_index=False)
        .apply(lr_params_as_series, "odometer", soh_col, include_groups=False)
    )
    return pd.Series({
        "std_soh_diff_to_lr": results.merge(lr_params, "left", "vin").eval(f"intercept + odometer * slope - {soh_col}").std(),
        "MAE_to_aviloo_soh_readouts": results.groupby("vin", observed=True).agg({soh_col:"median"}).merge(aviloo_readouts, "left", "vin").eval(f"soh_readout - {soh_col}").abs().mean(),
        "MAE_to_base_trendline": lr_params.eval("slope - @BASE_SLOPE").abs().mean(),
        "MAE_to_1_intercept": lr_params["intercept"].sub(1).abs().mean(),
    })

def force_decay(df, window_size=3, max_drop=0.003):
    """
    Génère une série strictement décroissante à partir d'une liste de valeurs en :
      - Calcule d'abord une moyenne mobile,
      - En choisissant un point de départ proche du maximum (pour refléter les meilleures valeurs),
      - Puis en s'assurant qu'entre deux points consécutifs, la diminution ne dépasse pas max_drop,
        et que la série ne stagne pas sur plus de 2 points.
    """
    # Calcul de la moyenne mobile
    smoothed = df['soh'].rolling(window=window_size, min_periods=2).mean().values
    # récupère l'odomètre pour la dcroissance forcé.
    odometer = df['odometer'].ffill().copy().values
    n = len(smoothed)
    
    # Pour le démarrage, on part du maximum de la série lissée / pas forcéement optimal
    output = [max(smoothed[1:])]
    odometer_fill = [odometer[1]]
    for i in range(1, n):
        candidate = smoothed[i]
        prev = output[-1]
        
        # Si  candidate >= à la précédente, on la force à être légèrement plus basse en se basant sur l'odomètre
        if candidate >= prev:
            epsilon = (odometer[i] - odometer[i-1]) * 1e-7
            candidate = prev - epsilon

        
        # Vérifier que le drop n'est pas trop important
        drop = prev - candidate
        if drop > max_drop:
            # On cheeck qu'il y'a une variation de 1000km mini pour appliquer le drop 
            if odometer[i] - odometer_fill[-1] < 1000:
                candidate = prev
            candidate = prev - max_drop
        output.append(candidate)
        
    return output


def estimate_cycles(total_range:float=0, initial_range:float=1, soh:float=1.0):
    """Calcule le nombre estimé de cycles

    Args:
        total_range (float): nombre de km parcouru
        initial_range (float): autonomie initiale du véhicule
        soh (float, optional): SoH du véhicule 

    Returns:
        float: le nombre de cycle de la batterie
    """
    if soh is np.nan:
        soh=1
    try:
        total_cycle = total_range / (initial_range * (soh + 1) / 2)
        return round(total_cycle)
    except:
        return np.nan


def log_function(x, a ,b, c):
    return a + b * np.log1p(x / c)

def compute_confidence_interval(df):
    n = df.size
    mean = df.mean()
    std = df.std()
    median = df.median()
    
    if n == 0:
        return pd.Series({
            'lower': np.nan,
            'upper': np.nan,
            'number_charges': 0,
            'soh_median': np.nan,
            'ic_point_diff': np.nan
        })

    margin = 1.96 * std / np.sqrt(n)
    lower = round(mean - margin, 4)
    upper = round(mean + margin, 4)
    
    return pd.Series({
        'lower': lower,
        'upper': upper,
        'number_charges': n,
        'soh_median': median,
        'ic_point_diff': round(upper - lower, 4)
    })
