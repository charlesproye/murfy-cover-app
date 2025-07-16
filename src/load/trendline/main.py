
import logging
import numpy as np
import pandas as pd
from sqlalchemy import text
from load.trendline.trendline_utils import *
from core.gsheet_utils import *
from rapidfuzz import process, fuzz
import re

def mapping_vehicle_type(type_car, oem_name, model_name, db_df, battery_capacity=None):
    """Map a given vehicle to the closest model identifier in the database.
    Args:
        type_car (str): type car to find match
        oem_name (str): oem car
        model_name (str): model car
        db_df (pd.DataFrame): db with all the model in dbeaver
        battery_capacity (str, optional): capacity car battery. Defaults to None.

    Returns:
        str: type le plus proche présent dans la db de vehicle_model
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
        
    # filtre sur l'oem
    subset = db_df[db_df['oem_name'] == oem_name].copy()

    # Trouver la meilleure correspondance
    # Retourne le modèle le plus proche score_cutoff fixé à 0.1 pour le moment pour être presque sur d'avoir un retour
    match_model = process.extractOne(model_name, subset['model_name'], scorer=fuzz.token_sort_ratio, score_cutoff=.1)
    if match_model :
        match_model_name, _, _ = match_model
        # filtre sur le nom du modèle
        subset = subset[subset['model_name']==match_model_name]
        # on cherche la batterie avec la capacité la + proche
        try:
            if battery_capacity:
                battery_target = float(battery_capacity.lower().replace('kwh', '').strip())
                subset["distance"] = (subset["capacity"] - battery_target).abs()
                closest_rows = subset[subset["distance"] == subset["distance"].min()]
            else:
                closest_rows = subset

            # Correspondance floue sur le type
            match_type = process.extractOne(type_car, closest_rows['type'], scorer=fuzz.token_sort_ratio)
            if match_type:
                _, _, index = match_type
                return closest_rows.iloc[index]["type"]

        # type le plus ressemblant sans batterie
        except:
            match_type = process.extractOne(type_car, subset['type'], scorer=fuzz.token_sort_ratio)
            _, _, index = match_type
            return subset.loc[index, "type"]
        
    raise Exception("unknown model")

def generate_trendline_functions(df, odometer_column, soh_column):
    """_summary_

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
    df = load_excel_data(get_gspread_client(), "202505 - Courbes SoH", "Courbes OS")
    df_sheet = pd.DataFrame(columns=df[0,:8], data=df[1:,:8])
    df_sheet['OEM'] = df_sheet['OEM'].apply(str.lower)
    df_sheet['Modèle'] = df_sheet['Modèle'].apply(str.lower)
    df_sheet["SoH"] = df_sheet["SoH"].apply(lambda x:  x.replace('%', '').strip()).astype(float) / 100
    df_sheet["Odomètre (km)"] = df_sheet["Odomètre (km)"].apply(lambda x:  str(x).replace(' ', '').strip()).astype(float)
    engine = get_sqlalchemy_engine()
    con = engine.connect()

    with engine.connect() as connection:
        dbeaver_df = pd.read_sql(text("""SELECT vm.model_name, vm.id, vm.type, vm.battery_id, o.oem_name, b.capacity  FROM vehicle_model vm
                                    join OEM o on vm.oem_id=o.id
                                    join battery b on b.id=vm.battery_id;"""), con)

    ### Matching type 
    df_sheet['type'] = df_sheet.apply(lambda row: mapping_vehicle_type(row['Type'], row['OEM'], str(row['Modèle']), dbeaver_df,  row['battery_capacity']), axis=1)
    
    df_sheet['Modèle'] = df_sheet['Modèle'].apply(lambda x: x.lower())
    df_merge = df_sheet.merge(dbeaver_df[['model_name', "type", 'battery_id']], left_on=['Modèle', "type"], right_on=['model_name', 'type'])
    df_merge['type'] = df_merge.groupby(['model_name', 'battery_id'])['type'].transform('first')
    for model_car in df_merge['Modèle'].unique()[:1]:
        print(model_car)
        for type_car in df_merge[df_merge['Modèle']==model_car].type.unique():
            try:
                mean_trend, upper_boun, lower_bound = generate_trendline_functions(df_merge[(df_merge['Modèle']==model_car) & (df_merge['type']==type_car)], "Odomètre (km)", "SoH")
                update_database_trendlines(model_car, type_car, mean_trend, upper_boun, lower_bound, False)
                logging.info(f"Trendline mise à jour pour {type_car}")
            except Exception as e: 
                logging.error(f"Erreur mise à jour trendline {type_car}: {e}")

######## Compute trendline from bib SoH ####################

    oems = ['tesla'] # add the oem with SoH from bib
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
            

        for model_car in df.model_name.unique():
            for type_car in df["type"].unique():
                try:
                    mean_trend, upper_boun, lower_bound = generate_trendline_functions(df[(df["model_name"] == model_car)  & (df['type']==type_car)], "odometer", "soh")
                    update_database_trendlines(model_car, type_car, mean_trend, upper_boun, lower_bound)
                except Exception as e:
                    logging.error(f"Erreur modèle {model_car}: {e}")
