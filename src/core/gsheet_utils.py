import os
import gspread
from google.oauth2.service_account import Credentials
import numpy as np
import pandas as pd
def get_gspread_client():
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    cred_path = os.path.join(root_dir, "src", "ingestion", "vehicle_info", "config", "config.json")
    creds = Credentials.from_service_account_file(
        cred_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets", 
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def clean_gsheet(gsheet, feuille, keep_first_line=True):
    client = get_gspread_client()

    spreadsheet = client.open(gsheet)
    worksheet = spreadsheet.worksheet(feuille)

    # Récupérer le nombre total de lignes et colonnes utilisées
    rows = worksheet.row_count
    cols = worksheet.col_count

    # Construire la plage à effacer (tout sauf la 1ère ligne)
    if keep_first_line is True:
        range_to_clear = f'A2:{chr(64 + cols)}{rows}' 
        worksheet.batch_clear([range_to_clear])
    else:
        worksheet.clear()
        
def load_excel_data(client, gsheet, feuille):
    sheet = client.open(gsheet)
    courbes_sheet = sheet.worksheet(feuille)
    sheet_data = np.array(courbes_sheet.get_all_values())

    
    return sheet_data
