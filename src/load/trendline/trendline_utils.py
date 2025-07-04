
import numpy as np
import json
from core.sql_utils import get_sqlalchemy_engine()
from scipy.optimize import curve_fit
from sqlalchemy.sql import text
from core.stats_utils import log_function


def compute_trendline_bounds(true, fit, window_size=50):
    local_std = np.array([
        np.std(true[max(0, i - window_size):min(len(true), i + window_size)])
        for i in range(len(true))
    ])
    smooth = np.linspace(local_std[0], local_std[-1], len(local_std))
    return fit - smooth, fit + smooth

def generate_trendline_functions(x_sorted, y_lower, y_upper, coef_min=None, coef_max=None):
    def log_func_min(x, a, b): return coef_max + a * np.log1p(x / b) if coef_min is not None else y_lower.max() + a * np.log1p(x / b)
    def log_func_max(x, a, b): return coef_min + a * np.log1p(x / b) if coef_max is not None else y_upper.max() + a * np.log1p(x / b)
    coef_lower, _ = curve_fit(log_func_min, x_sorted, y_lower, maxfev=10000)
    coef_upper, _ = curve_fit(log_func_max, x_sorted, y_upper, maxfev=10000)
    return coef_lower, coef_upper


def build_trendline_expressions(coef_mean, coef_lower, coef_upper, y_lower, y_upper):
    return (
        {"trendline": f"{coef_mean[0]} + {coef_mean[1]} * np.log1p(x/{coef_mean[2]})"},
        {"trendline": f"{max(y_upper.max(), 1)} + {coef_upper[0]} * np.log1p(x/{coef_upper[1]})"},
        {"trendline": f"{min(y_lower.max(), 1)} + {coef_lower[0]} * np.log1p(x/{coef_lower[1]})"}
    )


def update_database_trendlines(table, identifier_field, identifier, trendline_data):
    sql_request = text(f"""
        UPDATE {table}
        SET trendline = :trendline_json,
            trendline_min = :trendline_min_json,
            trendline_max = :trendline_max_json
        WHERE {identifier_field} = :identifier
    """)
    with get_sqlalchemy_engine().connect() as conn:
        conn.execute(sql_request, {
            "trendline_json": json.dumps(trendline_data[0]),
            "trendline_min_json": json.dumps(trendline_data[2]),
            "trendline_max_json": json.dumps(trendline_data[1]),
            "identifier": identifier
        })
    
def clean_battery_data(df, soh_colum, odometer_column):
    """
    Nettoie les données de batterie en supprimant les valeurs aberrantes.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame contenant les données de batterie avec des colonnes odometer et 'soh'
    soh_column: str
        Nom de la colonne qui contient les SoH
    odometer_column: str
        Nom de la colonne qui contient l'info sur l'odomètre
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame nettoyé
    """
    df_clean = df.copy()
    df_clean = df_clean.raname(columns={odometer_column: 'odometer', soh_colum:'soh'})
    df_clean = df_clean.drop(df_clean[(df_clean['odometer'] < 20000) & (df_clean['soh'] < .95)].index)
    df_clean = df_clean.drop(df_clean[(df_clean['soh'] < .8)].index)
    df_clean = df.dropna(subset=["soh", "odometer"])
    return df_clean


def get_model_name(df, dbeaver_df):
    """
    Récupère le nom du modèle à partir de l'ID du modèle.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame contenant la colonne 'model_id'
    dbeaver_df : pandas.DataFrame
        DataFrame de référence avec colonnes 'id', 'model_name', 'type'
        
    Returns:
    --------
    str
        Nom complet du modèle
    """
    id = df['model_id'].unique()
    model = f"{dbeaver_df[dbeaver_df['id'].astype(str) == str(id[0])]['model_name'].values[0]} {dbeaver_df[dbeaver_df['id'].astype(str) == str(id[0])]['type'].values[0]}"
    return model

def prepare_data_for_fitting(df):
    """
    Prépare les données pour le fitting en ajoutant le point d'origine et en triant.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame avec colonnes 'odometer' et 'soh'
        
    Returns:
    --------
    tuple
        (x_sorted, y_sorted) - données triées prêtes pour le fitting
    """
    x_data, y_data = df["odometer"].values, df["soh"].values
    x_data = np.hstack((x_data, np.array([0])))
    y_data = np.hstack((y_data, np.array([1])))
    sort_idx = np.argsort(x_data)
    x_sorted, y_sorted = x_data[sort_idx], y_data[sort_idx]
    return x_sorted, y_sorted


def compute_main_trendline(x_sorted, y_sorted):
    """
    Calcule la ligne de tendance principale et les bornes.
    
    Parameters:
    -----------
    x_sorted : numpy.array
        Données x triées
    y_sorted : numpy.array
        Données y triées
        
    Returns:
    --------
    tuple
        (coef_mean, coef_lower, coef_upper, mean, upper, lower)
    """
    coef_mean, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000, 
                           bounds=([.97, -np.inf, -np.inf], [1.03, np.inf, np.inf]))
    y_fit = log_function(x_sorted, *coef_mean)
    y_lower, y_upper = compute_trendline_bounds(y_sorted, y_fit)
    coef_lower, coef_upper = generate_trendline_functions(coef_mean[0], x_sorted, y_lower, y_upper)
    mean, upper, lower = build_trendline_expressions(coef_mean, coef_lower, coef_upper, y_lower, y_upper)
    return coef_mean, coef_lower, coef_upper,  mean, upper, lower


def compute_upper_bound(df, trendline, coef_mean):
    """
    Calcule la borne supérieure si nécessaire.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame des données
    trendline : str
        Equation de la trendline moyenne
    coef_mean : numpy.array
        Coefficients moyens
        
    Returns:
    --------
    dict or None
        Borne supérieure calculée ou None
    """
    print(trendline)
    mask = eval(trendline['trendline'], {"np": np, "x": df["odometer"]})
    test = df[df['soh'] > mask]
    x_sorted, y_sorted = prepare_data_for_fitting(test)
    
    coef_mean_upper, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000, 
                                bounds=([.97, -np.inf, -np.inf], [1.03, np.inf, np.inf]))
    y_fit = log_function(x_sorted, *coef_mean_upper)
    y_lower, y_upper = compute_trendline_bounds(y_sorted, y_fit)
    coef_lower_borne_sup, coef_upper_borne_sup = generate_trendline_functions(coef_mean[0], x_sorted, y_lower, y_upper)
    new_upper = build_trendline_expressions(coef_mean, coef_lower_borne_sup, coef_upper_borne_sup, y_lower, y_upper)
    upper_bound = new_upper[1]
    return upper_bound



def compute_lower_bound(df, trendlines, coef_mean):
    """
    Calcule la borne inférieure si nécessaire.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame des données nettoyées
    coef_mean : numpy.array
        Coefficients moyens
    trendlines : list
        Liste des lignes de tendance
        
    Returns:
    --------
    dict or None
        Borne inférieure calculée ou None
    """

    mask = eval(trendlines['trendline'], {"np": np, "x": df["odometer"]})
    test = df[df['soh'] < mask]
    x_sorted, y_sorted = prepare_data_for_fitting(test)
    
    coef_mean_upper, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000, 
                                bounds=([.97, -np.inf, -np.inf], [1.03, np.inf, np.inf]))
    y_fit = log_function(x_sorted, *coef_mean_upper)
    y_lower, y_upper = compute_trendline_bounds(y_sorted, y_fit)
    coef_lower_borne_sup, coef_upper_borne_sup = generate_trendline_functions(coef_mean[0], x_sorted, y_lower, y_upper)
    new_upper = build_trendline_expressions(coef_mean, coef_lower_borne_sup, coef_upper_borne_sup, y_lower, y_upper)
    upper_bound = new_upper[1]
    return upper_bound



