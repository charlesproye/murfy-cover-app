import os
import gspread
from google.oauth2.service_account import Credentials
import numpy as np
import json 
import base64
import logging
from logging import getLogger
from typing import Any, List


logger = getLogger("ingestion.vehicle_info")

def get_google_client() -> Any:
    """Get authenticated Google Sheets client.

    Returns:
        gspread.Client: Authenticated Google Sheets client
    """
    try:
        base64_creds = os.getenv("GOOGLE_PRIVATE_KEY")
        if not base64_creds:
            raise ValueError("GOOGLE_PRIVATE_KEY not found in environment variables")

        # Décoder et parser les credentials
        creds_dict = json.loads(base64.b64decode(base64_creds))

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=scopes
        )

        return gspread.authorize(credentials)

        
    except Exception as e:
        logging.error(f"Failed to get Google Sheets client: {str(e)}")
        raise

def clean_gsheet(gsheet, feuille, keep_first_line=True):
    client = get_google_client()

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

def export_to_excel(df_to_write, gsheet, feuille):
    client = get_google_client()
    sheet_out = client.open(gsheet)
    worksheet = sheet_out.worksheet(feuille)
    worksheet.append_rows(df_to_write.values.tolist())
    print(f"Données écritent dans {gsheet} {feuille}")
