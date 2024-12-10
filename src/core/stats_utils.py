from logging import Logger, getLogger

from scipy.stats import linregress as lr
import numpy as np
from scipy.optimize import minimize

from core.pandas_utils import *

logger = getLogger("core.stats_utils")

def filter_results_by_lines_bounds(results: DF, valid_soh_points: DF, logger: Logger) -> DF:
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
    rows_removed_pct = 100 * nb_rows_removed / results.shape[0]
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


def detecter_outliers(soh_values, method="iqr"):
    """
    Détecte les outliers dans une série de données.
    Paramètres :
        - soh_values : Liste des valeurs de SoH brutes.
        - method : Méthode de détection ("iqr" ou "std").
    Retourne :
        - Liste des indices non considérés comme outliers.
    """
    soh_values = np.array(soh_values)
    if method == "iqr":
        # Méthode de l'écart interquartile (IQR)
        q1, q3 = np.percentile(soh_values, [25, 75])
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
    elif method == "std":
        # Méthode basée sur l'écart-type
        mean = np.mean(soh_values)
        std_dev = np.std(soh_values)
        lower_bound = mean - 3 * std_dev
        upper_bound = mean + 3 * std_dev
    else:
        raise ValueError("Méthode non reconnue. Utilisez 'iqr' ou 'std'.")
    
    # Indices des valeurs non considérées comme outliers
    return [i for i, val in enumerate(soh_values) if lower_bound <= val <= upper_bound]

def optimiser_soh(soh_brut):
    """
    Ajuste les valeurs de SoH pour garantir une décroissance tout en minimisant 
    l'écart avec les valeurs brutes.
    """
    n = len(soh_brut)
    
    # Fonction objectif : minimiser l'écart quadratique entre les valeurs ajustées et les valeurs brutes
    def objectif(soh_adjusted):
        return np.sum((soh_adjusted - soh_brut) ** 2)
    
   # Contraintes : SoH doit être non-croissant et l'écart hebdomadaire doit être inférieur à 0,1%
    contraintes = [
        {'type': 'ineq', 'fun': lambda x, i=i: x[i - 1] - x[i]} for i in range(1, n)
    ] + [
        {'type': 'ineq', 'fun': lambda x, i=i: 0.001 * x[i - 1] - (x[i - 1] - x[i])} for i in range(1, n)
    ]
    
    # Borne supérieure et inférieure (par exemple, entre 0 et 100)
    bornes = [(0, 100) for _ in range(n)]
    
    # Initialisation des valeurs ajustées (on commence par les valeurs brutes)
    initial_guess = soh_brut.copy()
    
    # Résolution de l'optimisation
    result = minimize(objectif, initial_guess, method='SLSQP', constraints=contraintes, bounds=bornes)
    
    if result.success:
        return result.x  # Retourne les valeurs ajustées
    else:
        raise ValueError("L'optimisation a échoué : ", result.message)

def process_soh_for_vehicle(group):
    """
    Traite les données SoH pour un véhicule spécifique.
    """
    soh_values = group['soh'].values
    
    # Détection des outliers
    indices_valides = detecter_outliers(soh_values, method="iqr")
    soh_nettoye = np.array([soh_values[i] for i in indices_valides])
    
    # Optimisation
    if len(soh_nettoye) > 0:
        soh_optimise = optimiser_soh(soh_nettoye)
        
        # Création d'un nouveau DataFrame avec les valeurs optimisées
        result = group.iloc[indices_valides].copy()
        result['soh'] = soh_optimise
        return result
    return group


