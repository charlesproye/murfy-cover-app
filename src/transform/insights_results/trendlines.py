import numpy as np
from scipy.optimize import curve_fit
from core.sql_utils import *
import json
import logging
import os
import gspread
from google.oauth2.service_account import Credentials


root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
cred_path = os.path.join(root_dir, "ingestion", "vehicle_info", "config", "config.json")

CREDS = Credentials.from_service_account_file(
    cred_path,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)

def log_function(x, a ,b, c):
    return a + b * np.log1p(x / c)


def get_trendlines(df, oem=None, version=None, update=False, source='dbeaver'):
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

    if df.dropna(subset=['soh', 'odometer']).shape[0] < 1:
        logging.warning(f"Pas de SoH pour calculer une trendline pour {oem} {version}")
        return 

    logging.info(f"Calcul de la trendline pour {oem} {version}")
    
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

    trendline = {"trendline": f"{coef_[0]} + {coef_[1]} * np.log1p(x/{coef_[2]})"}
    trendline_max = {"trendline": f"{max(y_upper.max(), 1)}+ {coef_upper[0]} * np.log1p(x/{coef_upper[1]})"}
    trendline_min = {"trendline": f"{min(y_lower.max(), 1)} + {coef_lower[0]} * np.log1p(x/ {coef_lower[1]})"}

    # update des données dans dbeaver
    if update is True:
        logging.info(f"Update data in database for {oem}")
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
            logging.info(f"Update data in database for {version}")
            sql_request = text("""
                UPDATE vehicle_model 
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

    # update oem
    for oem_name in ["ford", "bmw", "kia", "stellantis", "mercedes-benz", "volvo-cars", 
                     "renault", "mercedes", "volvo", "volkswagen", "toyota", "tesla"]:
        with engine.connect() as connection:
            query = text("""
                SELECT *  
                FROM vehicle v	
                JOIN vehicle_model vm ON vm.id = v.vehicle_model_id
                JOIN vehicle_data vd ON vd.vehicle_id = v.id
                JOIN oem o ON o.id = vm.oem_id
                JOIN battery b ON b.id = vm.battery_id
                WHERE o.oem_name = :oem_name
            """)

            dbeaver_df = pd.read_sql(query, connection, params={"oem_name": oem_name})
            
        ## trendline oem     
        #get_trendlines(dbeaver_df, oem=oem_name, update=True)
        
        ## update version

        courbe_model = {}
        for model in dbeaver_df.model_name.unique():
            courbe_model[model] = get_trendlines(dbeaver_df[dbeaver_df['model_name']==model])

        for version in dbeaver_df.version.unique():
            if version == "unknown":
                continue

            version_df = dbeaver_df[dbeaver_df["version"] == version]
            model_name = version_df["model_name"].iloc[0]

            if version_df["trendline_active"].iloc[-1] is True:
                get_trendlines(version_df, version=version, update=True)
                logging.warning("Update google sheet")
                df_to_write = pd.DataFrame(version_df[['oem_name', 'model_name', 'type', 'version']].iloc[-1]).T
                df_to_write['source'] = "dbeaver"
                client = gspread.authorize(CREDS)
                sheet = client.open("BP - Rapport Freemium")  
                worksheet = sheet.worksheet("Trendline")
                worksheet.append_rows(df_to_write.values.tolist())
                
            else:
                if courbe_model[model_name] is not None:
                    logging.warning(f"Pas assez de données pour {version}, utilisation de la trendline du modèle {model_name}")
                    trendline, trendline_max, trendline_min = courbe_model[model_name]
                    sql_request = text("""
                        UPDATE vehicle_model 
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
