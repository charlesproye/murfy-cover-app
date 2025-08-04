
import numpy as np
import json
from core.sql_utils import get_sqlalchemy_engine
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

def get_bound_coef(x_sorted, y_lower, y_upper, coef_min=None, coef_max=None):
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


def update_database_trendlines(model_car, type_car, mean_trendline, upper_trendline, lower_trendline, trendline_bib=True):
    sql_request = text(f"""
        UPDATE vehicle_model
        SET trendline = :trendline_json,
            trendline_min = :trendline_min_json,
            trendline_max = :trendline_max_json,
            trendline_bib = :trendline_bib
        WHERE model_name = :model 
        AND type = :type
    """)
    with get_sqlalchemy_engine().begin() as conn:
        conn.execute(sql_request, {
            "trendline_json": json.dumps({"trendline" : mean_trendline}),
            "trendline_min_json": json.dumps({"trendline" : upper_trendline}),
            "trendline_max_json": json.dumps({"trendline" : lower_trendline}),
            "model": model_car,
            "type": type_car,
            "trendline_bib": trendline_bib
        })
    
def clean_battery_data(df, odometer_column, soh_colum):
    """
    Cleans battery data by removing outliers.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing battery data with 'odometer' and 'soh' columns.
    soh_column : str
        Name of the column that contains the State of Health (SoH).
    odometer_column : str
        Name of the column that contains the odometer information.
        
    Returns:
    --------
    pandas.DataFrame
        Cleaned DataFrame.
    """
    df_clean = df.copy()
    df_clean = df_clean.rename(columns={odometer_column: 'odometer', soh_colum:'soh'})
    df_clean = df_clean.drop(df_clean[(df_clean['odometer'] < 20000) & (df_clean['soh'] < .95)].index)
    df_clean = df_clean.drop(df_clean[(df_clean['soh'] < .8)].index)
    df_clean = df_clean.dropna(subset=["soh", "odometer"])
    return df_clean


def get_model_name(df, dbeaver_df):
    """
    Retrieves the model name from the model ID.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the 'model_id' column.
    dbeaver_df : pandas.DataFrame
        Reference DataFrame with columns 'id', 'model_name', and 'type'.
        
    Returns:
    --------
    str
        Full model name.
    """
    id = df['model_id'].unique()
    model = f"{dbeaver_df[dbeaver_df['id'].astype(str) == str(id[0])]['model_name'].values[0]} {dbeaver_df[dbeaver_df['id'].astype(str) == str(id[0])]['type'].values[0]}"
    return model

def prepare_data_for_fitting(df):
    """
    Prepares data for fitting by adding the origin point and sorting.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with 'odometer' and 'soh' columns.
        
    Returns:
    --------
    tuple
        (x_sorted, y_sorted) - sorted data ready for fitting.
    """
    x_data, y_data = df["odometer"].values, df["soh"].values
    x_data = np.hstack((x_data, np.array([0])))
    y_data = np.hstack((y_data, np.array([1])))
    sort_idx = np.argsort(x_data)
    x_sorted, y_sorted = x_data[sort_idx], y_data[sort_idx]
    return x_sorted, y_sorted


def compute_main_trendline(x_sorted, y_sorted):
    """
    Computes the main trend line and the bounds.
    
    Parameters:
    -----------
    x_sorted : numpy.array
        Sorted x data.
    y_sorted : numpy.array
        Sorted y data.
        
    Returns:
    --------
    tuple
        (coef_mean, coef_lower, coef_upper, mean, upper, lower)
    """
    coef_mean, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000, 
                           bounds=([.97, -np.inf, -np.inf], [1.03, np.inf, np.inf]))
    y_fit = log_function(x_sorted, *coef_mean)
    y_lower, y_upper = compute_trendline_bounds(y_sorted, y_fit)
    coef_lower, coef_upper = get_bound_coef(x_sorted, y_lower, y_upper)
    mean, upper, lower = build_trendline_expressions(coef_mean, coef_lower, coef_upper, y_lower, y_upper)
    return coef_mean, coef_lower, coef_upper,  mean, upper, lower


def compute_upper_bound(df, trendline, coef_mean):
    """
    Computes the upper bound if necessary.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the data.
    trendline : str
        Equation of the mean trendline.
    coef_mean : numpy.array
        Mean coefficients.
        
    Returns:
    --------
    dict or None
        Computed upper bound or None.
    """
    mask = eval(trendline['trendline'], {"np": np, "x": df["odometer"]})
    test = df[df['soh'] > mask]
    x_sorted, y_sorted = prepare_data_for_fitting(test)
    
    coef_mean_upper, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000, 
                                bounds=([.97, -np.inf, -np.inf], [1.03, np.inf, np.inf]))
    y_fit = log_function(x_sorted, *coef_mean_upper)
    y_lower, y_upper = compute_trendline_bounds(y_sorted, y_fit)
    coef_lower_borne_sup, coef_upper_borne_sup = get_bound_coef(coef_mean[0], x_sorted, y_lower, y_upper)
    new_upper = build_trendline_expressions(coef_mean, coef_lower_borne_sup, coef_upper_borne_sup, y_lower, y_upper)
    upper_bound = new_upper[1]
    return upper_bound



def compute_lower_bound(df, trendlines, coef_mean):
    """
    Computes the lower bound if necessary.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Cleaned data DataFrame.
    coef_mean : numpy.array
        Mean coefficients.
    trendlines : list
        List of trendlines.
        
    Returns:
    --------
    dict or None
        Computed lower bound or None.
    """
    mask = eval(trendlines['trendline'], {"np": np, "x": df["odometer"]})
    test = df[df['soh'] < mask]
    x_sorted, y_sorted = prepare_data_for_fitting(test)
    
    coef_mean_upper, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000, 
                                bounds=([.97, -np.inf, -np.inf], [1.03, np.inf, np.inf]))
    y_fit = log_function(x_sorted, *coef_mean_upper)
    y_lower, y_upper = compute_trendline_bounds(y_sorted, y_fit)
    coef_lower_borne_sup, coef_upper_borne_sup = get_bound_coef(coef_mean[0], x_sorted, y_lower, y_upper)
    new_upper = build_trendline_expressions(coef_mean, coef_lower_borne_sup, coef_upper_borne_sup, y_lower, y_upper)
    upper_bound = new_upper[1]
    return upper_bound


def filtrer_trendlines(df, col_odometer='odometer', col_vin='vin', km_lower=80000, km_upper=100000, vin_total=50, nbr_under=20, nbr_upper=10):
    """
    Filters models based on mileage and the number of unique listings.

    Parameters:
    - df: DataFrame with the data point
    - km_low: maximum mileage for low-mileage vehicles (default: 80,000 km)
    - km_high: minimum mileage for high-mileage vehicles (default: 100,000 km)
    - min_total: minimum number of unique listings required for a model
    - min_low: minimum number of low-mileage listings required
    - min_high: minimum number of high-mileage listings required

    Returns:
    - A list of tuples (Modèle, model_id) that meet the criteria
    """
    
    nb_total_vins = df[col_vin].nunique()
    nb_lower = df[df[col_odometer] <= km_lower][col_vin].nunique()
    nb_upper = df[df[col_odometer] >= km_upper][col_vin].nunique()
    
    if nb_total_vins >= vin_total and nb_lower >= nbr_under and nb_upper >= nbr_upper:
        return nb_total_vins
    print("The model don't respect the filtering criteria")


