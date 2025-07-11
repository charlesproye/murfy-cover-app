import os
import logging
from typing import Any, List
import gspread
from google.oauth2.service_account import Credentials
from logging import getLogger

logger = getLogger("ingestion.vehicle_info")

SPREADSHEET_ID = "1zGwSY41eN00YQbaNf9HNk3g5g6KQaAD1FY7-XS8Uf9w"

def get_google_client() -> Any:
    """Get authenticated Google Sheets client.
    
    Returns:
        gspread.Client: Authenticated Google Sheets client
    """
    try:
        creds_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'config',
            'config.json'
        )
        
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = Credentials.from_service_account_file(
            creds_path,
            scopes=scopes
        )
        
        return gspread.authorize(credentials)
        
    except Exception as e:
        logging.error(f"Failed to get Google Sheets client: {str(e)}")
        raise

def update_real_activation_status(vins: List[str], status: bool = False) -> None:
    """Update the 'Real Activation' column for specified VINs in batch
    
    Args:
        vins: List of VINs to update
        status: True to set Real Activation to TRUE, False to set it to FALSE
    """
    if not vins:
        return

    try:
        client = get_google_client()
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        
        all_data = sheet.get_all_values()
        headers = all_data[0]
        
        vin_col_idx = headers.index('VIN')
        real_activation_col_idx = headers.index('Real Activation')
        
        cells_to_update = []
        for row_idx, row in enumerate(all_data[1:], start=2):
            if row[vin_col_idx] in vins:
                cells_to_update.append({
                    'range': f'R{row_idx}C{real_activation_col_idx + 1}',
                    'values': [['TRUE' if status else 'FALSE']]
                })
                logger.info(f"Preparing to update Real Activation status to {status} for VIN: {row[vin_col_idx]}")
        
        if cells_to_update:
            sheet.batch_update(cells_to_update)
            logger.info(f"Successfully updated {len(cells_to_update)} vehicles' Real Activation status to {status}")
            
    except Exception as e:
        logger.error(f"Error updating Real Activation status: {str(e)}")

