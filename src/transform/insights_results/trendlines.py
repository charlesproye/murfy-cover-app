import numpy as np
from scipy.optimize import curve_fit
from core.sql_utils import *
import json

def log_function(x, a ,b):
    return 1 + a * np.log1p(x / b)


def get_trendlines(df, oem=None, version=None, update=False):
    """
    Computes logarithmic trendlines for the relationship between odometer and SOH (State of Health)
    from a given DataFrame, and updates the results in a database if `oem` or `version` are specified.

    The function:
    - Fits a logarithmic curve to the data (mean trendline).
    - Calculates upper and lower bounds using smoothed local standard deviation.
    - Generates expressions for mean, max, and min trendlines.
    - Optionally updates these trendlines in the database for a given OEM or vehicle model version.

    Parameters:
    ----------
    df : pandas.DataFrame
        A DataFrame containing at least two columns: 'odometer' and 'soh'.

    oem : str, optional
        The name of the OEM (Original Equipment Manufacturer) for which to update the trendlines in the database.

    version : str, optional
        The vehicle model version for which to update the trendlines in the database.

    Returns:
    -------
    tuple
        A tuple of three dictionaries containing the string expressions of the mean, max, and min trendlines:
        (trendline, trendline_max, trendline_min)
    """

    # On utilise dropna pour ne pas avoir de problèmes de calcul par la suite
    x_data = df.dropna(subset=['soh', 'odometer'])['odometer'].values
    y_data = df.dropna(subset=['soh', 'odometer'])['soh'].values

    # Tri des données
    sort_idx = np.argsort(x_data)
    x_sorted = x_data[sort_idx]
    y_sorted = y_data[sort_idx]

    # Ajustement de la fonction sur les données
    coef_, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000)
    y_fit = log_function(x_sorted, *coef_)

    # Calcul de l'écart-type local
    window_size = 50
    local_std = np.array([
        np.std(y_sorted[max(0, i-window_size):min(len(y_sorted), i+window_size)])
        for i in range(len(y_sorted))
    ])

    # Interpolation linéaire entre le premier et le dernier point
    smooth = np.linspace(local_std[0], local_std[-1], len(local_std))

    # Calcul des intervalles de confiance & création des trendlines associées
    # On peut augmenter le coef devant smooth pour augmenter la taille des intervalles
    y_lower = y_fit - 1 * smooth
    y_upper = y_fit +  1 * smooth


    def log_function_min(x, a, b):
        return y_lower.max() + a * np.log1p(x/b)
    def log_function_max(x, a, b):
        return y_upper.max() + a * np.log1p(x/b)

    coef_lower, _ = curve_fit(log_function_min, x_sorted, y_lower, maxfev=10000)
    coef_upper, _ = curve_fit(log_function_max, x_sorted, y_upper, maxfev=10000)

    trendline = {"trendline": f"1 + {coef_[0]} * np.log1p(x/{coef_[1]})"}
    trendline_max ={"trendline": f"{y_upper.max()}+ {coef_upper[0]} * np.log1p(x/{coef_upper[1]})"}
    trendline_min ={"trendline": f"{y_lower.max()} + {coef_lower[0]} * np.log1p(x/ {coef_lower[1]})"}

    # update des données dans dbeaver
    if update is True:
        if oem:
            sql_request = text("""
                UPDATE oem 
                SET trendline = :trendline_json,
                    trendline_min = :trendline_min_json,
                    trendline_max = :trendline_max_json
                WHERE oem_name = :oem_name
            """)

            with engine.begin() as conn:
                conn.execute(sql_request, {
                    "trendline_json": json.dumps(trendline),
                    "trendline_max_json": json.dumps(trendline_max),
                    "trendline_min_json": json.dumps(trendline_min),
                    "oem_name": oem
                })

        if version:
            sql_request = text("""
                UPDATE vehicule_model 
                SET trendline = :trendline_json,
                    trendline_min = :trendline_min_json,
                    trendline_max = :trendline_max_json
                WHERE version = :version
            """)

            with engine.begin() as conn:
                conn.execute(sql_request, {
                    "trendline_json": json.dumps(trendline),
                    "trendline_max_json": json.dumps(trendline_max),
                    "trendline_min_json": json.dumps(trendline_min),
                    "version": version
                })
    return trendline, trendline_max, trendline_min

if __name__ == "__main__":
    pass
    
