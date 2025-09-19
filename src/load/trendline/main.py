
import logging
import numpy as np
import pandas as pd
from sqlalchemy import text
from load.trendline.trendline_utils import *
from core.gsheet_utils import *
from core.sql_utils import get_connection


def generate_trendline_functions(df, odometer_column, soh_column):
    """

    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe with SoH and Odometer column
    soh_column: str
        Nom de la colonne qui contient les SoH
    odometer_column: str
        Nom de la colonne qui contient l'info sur l'odomètre
    Returns:
    --------
    tupple
        trendlines moyenne, borne supérieure et inférieure
    """
    df_clean = clean_battery_data(df, odometer_column, soh_column)
    if df_clean.shape[0] < 20:
        return "Can't compute trendline"
    x_data, y_data = prepare_data_for_fitting(df_clean)
    coef_mean, coef_lower, coef_upper, mean, upper_bound, lower_bound = compute_main_trendline(x_data, y_data)
    if coef_upper[0] >= 0:
        upper_bound = compute_upper_bound(df_clean, mean, coef_mean)
    if  coef_lower[0] >= 0:
        lower_bound = compute_lower_bound(df_clean, mean, coef_mean)
    return mean, upper_bound, lower_bound



if __name__ == "__main__":
    
    
######## Compute trendline from scrapping SoH ####################    
    
    #### Get data from scrapping
    df = load_excel_data("Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=df[0,:8], data=df[1:,:8])
    df_sheet['OEM'] = df_sheet['OEM'].apply(str.lower)
    df_sheet['Modèle'] = df_sheet['Modèle'].apply(str.lower)
    df_sheet["SoH"] = df_sheet["SoH"].apply(lambda x:  x.replace('%', '').strip()).astype(float) / 100
    df_sheet["Odomètre (km)"] = df_sheet["Odomètre (km)"].apply(lambda x:  str(x).replace(',', '').strip()).astype(float)
    engine = get_sqlalchemy_engine()
    con = engine.connect()

    with engine.connect() as connection:
        dbeaver_df = pd.read_sql(text("""SELECT vm.model_name, vm.id, vm.type, m.make_name FROM vehicle_model vm
                                    join make m on m.id=vm.make_id;"""), con)
        df_merge = df_sheet.merge(dbeaver_df, right_on=['make_name', 'model_name', 'type'], left_on=['OEM', 'Modèle', 'Type'], how='inner')
        
    logging.info(f"Starting trendline update from gsheet")
    
    for model_car in df_merge['id'].unique():
        df_temp = df_merge[(df_merge['id']==model_car)].copy()
        try:
            if filtrer_trendlines(df_temp,  "Odomètre (km)", "lien", 50_000, 50_000, 50, 20, 10):
                mean_trend, upper_bound, lower_bound = generate_trendline_functions(df_temp, "Odomètre (km)", "SoH")
                update_database_trendlines(model_car, mean_trend, upper_bound, lower_bound, False)
                logging.info(f"Trendline update for car model {model_car}")
        except Exception as e: 
            logging.error(f"Error with car model: {model_car}: {e}")

######## Compute trendline from bib SoH ####################

    oems = ['tesla'] # add the oem with SoH from bib
    for oem_name in oems:
        with get_connection() as connection:
            query = """
                SELECT * FROM vehicle v
                JOIN vehicle_model vm ON vm.id = v.vehicle_model_id
                JOIN vehicle_data vd ON vd.vehicle_id = v.id
                JOIN oem o ON o.id = vm.oem_id
                JOIN battery b ON b.id = vm.battery_id
                WHERE o.oem_name = %s
            """
            df = pd.read_sql(query, connection, params=(oem_name,))
            
        logging.info(f"Starting trendline update from dbeaver")
        
        for model_car in df['vehicle_model_id'].unique():
            df_temp = df[(df['vehicle_model_id']==model_car)].copy()
            try:
                if filtrer_trendlines(df_temp,  "odometer", "vin", 50_000, 50_000, 50, 20, 10):
                    mean_trend, upper_bound, lower_bound = generate_trendline_functions(df_temp, "odometer", "soh")
                    update_database_trendlines(model_car, mean_trend, upper_bound, lower_bound)
            except Exception as e:
                logging.error(f"Error with car model: {model_car}: {e}")
