import os
from google.oauth2.service_account import Credentials
import numpy as np
import json 
import base64
import logging
from logging import getLogger
from typing import Any
import gspread

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
        raise ValueError(f"Failed to get Google Sheets client: {str(e)}") from e

def clean_gsheet(gsheet, feuille, keep_first_line=True):
    """erased data from the gsheet

    Args:
        gsheet (str): name of the gsheet
        feuille (str): name of the sheet 
        keep_first_line (bool, optional): True if you want to keep the first line. Defaults to True.
    """
    client = get_google_client()

    spreadsheet = client.open(gsheet)
    worksheet = spreadsheet.worksheet(feuille)
    rows = worksheet.row_count
    cols = worksheet.col_count

    if keep_first_line is True:
        range_to_clear = f'A2:{chr(64 + cols)}{rows}' 
        worksheet.batch_clear([range_to_clear])
    else:
        worksheet.clear()
        
def load_excel_data(gsheet, feuille):
    client = get_google_client()
    sheet = client.open(gsheet)
    courbes_sheet = sheet.worksheet(feuille)
    sheet_data = np.array(courbes_sheet.get_all_values())
    return sheet_data

def export_to_excel(df_to_write, gsheet, feuille):
    client = get_google_client()
    sheet_out = client.open(gsheet)
    worksheet = sheet_out.worksheet(feuille)
    worksheet.append_rows(df_to_write.values.tolist())
    logging.info("Data written in %s %s", gsheet, feuille)

