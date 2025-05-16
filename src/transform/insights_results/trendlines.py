import os
import json
import logging
import numpy as np
import pandas as pd
import gspread

from scipy.optimize import curve_fit
from google.oauth2.service_account import Credentials
from sqlalchemy.sql import text
from core.sql_utils import engine
from core.stats_utils import log_function

logging.basicConfig(level=logging.INFO)

def get_gspread_client():
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    cred_path = os.path.join(root_dir, "ingestion", "vehicle_info", "config", "config.json")
    creds = Credentials.from_service_account_file(
        cred_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets", 
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def compute_trendline_bounds(true, fit, window_size=50):
    local_std = np.array([
        np.std(true[max(0, i - window_size):min(len(true), i + window_size)])
        for i in range(len(true))
    ])
    smooth = np.linspace(local_std[0], local_std[-1], len(local_std))
    return fit - smooth, fit + smooth

def generate_trendline_functions(x_sorted, y_lower, y_upper):
    def log_func_min(x, a, b): return y_lower.max() + a * np.log1p(x / b)
    def log_func_max(x, a, b): return y_upper.max() + a * np.log1p(x / b)
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
    with engine.begin() as conn:
        conn.execute(sql_request, {
            "trendline_json": json.dumps(trendline_data[0]),
            "trendline_min_json": json.dumps(trendline_data[2]),
            "trendline_max_json": json.dumps(trendline_data[1]),
            "identifier": identifier
        })

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
    df_clean = df.dropna(subset=["soh", "odometer"])
    if df_clean.empty:
        logging.warning(f"Aucune donnée pour {oem} {version}")
        return

    x_data, y_data = df_clean["odometer"].values, df_clean["soh"].values
    sort_idx = np.argsort(x_data)
    x_sorted, y_sorted = x_data[sort_idx], y_data[sort_idx]

    coef_mean, _ = curve_fit(log_function, x_sorted, y_sorted, maxfev=10000)
    y_fit = log_function(x_sorted, *coef_mean)
    y_lower, y_upper = compute_trendline_bounds(x_sorted, y_sorted, y_fit)

    coef_lower, coef_upper = generate_trendline_functions(x_sorted, y_lower, y_upper)
    trendlines = build_trendline_expressions(coef_mean, coef_lower, coef_upper, y_lower, y_upper)

    if update:
        logging.info(f"Update trendlines for {oem or version}")
        if oem:
            update_database_trendlines("oem", "oem_name", oem, trendlines)
        if version:
            update_database_trendlines("vehicle_model", "version", version, trendlines)

    return trendlines

def run_trendline_main():
    client = get_gspread_client()
    oems = ["ford", "bmw", "kia", "stellantis", "mercedes-benz", "volvo-cars",
            "renault", "mercedes", "volvo", "volkswagen", "toyota", "tesla"]

    for oem_name in oems:
        with engine.connect() as connection:
            query = text("""
                SELECT * FROM vehicle v
                JOIN vehicle_model vm ON vm.id = v.vehicle_model_id
                JOIN vehicle_data vd ON vd.vehicle_id = v.id
                JOIN oem o ON o.id = vm.oem_id
                JOIN battery b ON b.id = vm.battery_id
                WHERE o.oem_name = :oem_name
            """)
            df = pd.read_sql(query, connection, params={"oem_name": oem_name})

        model_trends = {}
        for model in df["model_name"].unique():
            try:
                model_trends[model] = get_trendlines(df[df["model_name"] == model])
            except Exception as e:
                logging.error(f"Erreur modèle {model}: {e}")
                model_trends[model] = None

        for version in df["version"].unique():
            if version == "unknown":
                continue

            version_df = df[df["version"] == version]
            if version_df.empty:
                continue

            model_name = version_df["model_name"].iloc[0]
            if version_df["trendline_active"].iloc[-1]:
                try:
                    get_trendlines(version_df, version=version, update=True)
                    logging.info(f"Trendline mise à jour pour {version}")

                    info_df = version_df[['oem_name', 'model_name', 'type', 'version']].iloc[[-1]]
                    info_df['source'] = "dbeaver"
                    sheet = client.open("BP - Rapport Freemium")
                    worksheet = sheet.worksheet("Trendline")
                    worksheet.append_rows(info_df.values.tolist())
                except Exception as e:
                    logging.error(f"Erreur mise à jour trendline {version}: {e}")
            elif model_trends.get(model_name):
                try:
                    update_database_trendlines("vehicle_model", "version", version, model_trends[model_name])
                    logging.warning(f"Fallback trendline appliqué pour {version} (modèle {model_name})")
                except Exception as e:
                    logging.error(f"Erreur fallback pour {version}: {e}")


if __name__ == "__main__":
    run_trendline_main()
