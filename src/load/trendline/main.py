
import logging
import numpy as np
import pandas as pd
from sqlalchemy import text
from load.trendline.trendline_utils import *
from core.gsheet_utils import *
from rapidfuzz import process, fuzz
from core.sql_utils import get_connection
import re

def mapping_vehicle_type(type_car, oem_name, model_name, db_df, battery_capacity=None):
    """Map a given vehicle to the closest model identifier in the database.
    Args:
        type_car (str): type car to find match
        oem_name (str): oem car
        model_name (str): model car
        db_df (pd.DataFrame): db with all the model from dbeaver
        battery_capacity (str, optional): capacity car battery. Defaults to None.

    Returns:
        str: closest type found in the db 
    """
   
    oem_name = oem_name.lower()
    type_car = type_car.lower()
    try:
        if len(model_name) > 4:
            d = re.findall('\d*', model_name)
            d.sort()
            model_name = d[-1]
    except:
        model_name = model_name.lower()
        
    subset = db_df[db_df['oem_name'] == oem_name].copy()

    match_model = process.extractOne(model_name, subset['model_name'], scorer=fuzz.token_sort_ratio, score_cutoff=.1)
    if match_model :
        match_model_name, _, _ = match_model
        subset = subset[subset['model_name']==match_model_name]
        try:
            if battery_capacity:
                battery_target = float(battery_capacity.lower().replace('kwh', '').strip())
                subset["distance"] = (subset["capacity"] - battery_target).abs()
                closest_rows = subset[subset["distance"] == subset["distance"].min()]
            else:
                closest_rows = subset

            match_type = process.extractOne(type_car, closest_rows['type'], scorer=fuzz.token_sort_ratio)
            if match_type:
                _, _, index = match_type
                return closest_rows.iloc[index]["type"]
        except:
            match_type = process.extractOne(type_car, subset['type'], scorer=fuzz.token_sort_ratio)
            _, _, index = match_type
            return subset.loc[index, "type"]
        
    raise Exception("unknown model")

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
    df = load_excel_data(get_google_client(), "Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=df[0,:8], data=df[1:,:8])
    df_sheet['OEM'] = df_sheet['OEM'].apply(str.lower)
    df_sheet['Modèle'] = df_sheet['Modèle'].apply(str.lower)
    df_sheet["SoH"] = df_sheet["SoH"].apply(lambda x:  x.replace('%', '').strip()).astype(float) / 100
    df_sheet["Odomètre (km)"] = df_sheet["Odomètre (km)"].apply(lambda x:  str(x).replace(',', '').strip()).astype(float)
    engine = get_sqlalchemy_engine()
    con = engine.connect()

    with engine.connect() as connection:
        dbeaver_df = pd.read_sql(text("""SELECT vm.model_name, vm.id, vm.type, m.make_name FROM vehicle_model vm
                                    join make  on m.id=vm.make_id;"""), con)
        df_merge = df_sheet.merge(dbeaver_df, right_on=['make_name', 'model_name', 'type'], left_on=['OEM', 'Modèle', 'Type'], how='inner')

    for model_car in df_merge['id'].unique()[:1]:
        df_temp = df_merge[(df_merge['id']==model_car)].copy()
        try:
            if filtrer_trendlines(df_temp,  "Odomètre (km)", "lien", 50_000, 50_000, 50, 20, 10):
                mean_trend, upper_bound, lower_bound = generate_trendline_functions(df_temp, "Odomètre (km)", "SoH")
                update_database_trendlines(model_car, mean_trend, upper_bound, lower_bound, False)
                logging.info(f"Trendline mise à jour pour {model_car}")
        except Exception as e: 
            logging.error(f"Erreur mise à jour trendline {model_car}: {e}")

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
        for model_car in df['vehicle_model_id'].unique():
            df_temp = df[(df['vehicle_model_id']==model_car)].copy()
            try:
                if filtrer_trendlines(df_temp,  "odometer", "vin", 50_000, 50_000, 50, 20, 10):
                    mean_trend, upper_bound, lower_bound = generate_trendline_functions(df_temp, "odometer", "soh")
                    update_database_trendlines(model_car, mean_trend, upper_bound, lower_bound)
            except Exception as e:
                logging.error(f"Erreur modèle {model_car}: {e}")
