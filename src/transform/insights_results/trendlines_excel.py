import os
import logging
import gspread
import numpy as np
import pandas as pd
from google.oauth2.service_account import Credentials
from sqlalchemy import text

from transform.insights_results.trendlines import get_trendlines
from core.stats_utils import log_function
from core.sql_utils import engine

def get_gspread_client():
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    cred_path = os.path.join(root_dir, "ingestion", "vehicle_info", "config", "config.json")
    creds = Credentials.from_service_account_file(
        cred_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)


def load_excel_data(client):
    sheet = client.open("202505 - Courbes SoH")
    courbes_sheet = sheet.worksheet("Courbes OS")
    sheet_data = np.array(courbes_sheet.get_all_values())
    df_sheet = pd.DataFrame(sheet_data[1:, :6], columns=sheet_data[0, :6])

    df_sheet["Odomètre (km)"] = df_sheet["Odomètre (km)"].apply(lambda x: x.replace(" ", "")).astype(int)
    df_sheet["SoH"] = df_sheet["SoH"].apply(lambda x: x.replace("%", "")).astype(float)
    df_sheet.rename(columns={"Odomètre (km)": "odometer", "SoH": "soh"}, inplace=True)
    
    return df_sheet

def process_model_trendline(model, df_sheet, client):
    logging.info(f"Processing trendline for model: {model}")
    df_model = df_sheet[df_sheet["Modèle"] == model]

    with engine.connect() as connection:
        query = text("""
            SELECT *  
            FROM vehicle_model vm
            JOIN oem o ON o.id = vm.oem_id
            WHERE vm.model_name = :model
        """)
        dbeaver_df = pd.read_sql(query, connection, params={"model": model})
    
    if True in dbeaver_df['trendline_active'].values:
        get_trendlines(df_model, version=model, update=True)

        df_to_write = dbeaver_df[['oem_name', 'model_name', 'type']].iloc[[-1]]
        df_to_write['version'] = "None"
        df_to_write['source'] = "excel"

        sheet_out = client.open("BP - Rapport Freemium")
        worksheet = sheet_out.worksheet("Trendline")
        worksheet.append_rows(df_to_write.values.tolist())
        logging.info(f"Trendline written to sheet for model: {model}")
    else:
        logging.info(f"Trendline inactive for model: {model}")

def run_excel_trendlines():
    logging.basicConfig(level=logging.INFO)
    client = get_gspread_client()
    df_sheet = load_excel_data(client)

    for model in df_sheet["Modèle"].unique():
        try:
            process_model_trendline(model, df_sheet, client)
        except Exception as e:
            logging.error(f"Erreur lors du traitement du modèle {model} : {e}")

