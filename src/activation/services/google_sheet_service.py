import logging
from typing import Optional
from activation.config.credentials import SPREADSHEET_ID
from activation.utils.google_sheets_utils import get_google_client
import pandas as pd

async def update_vehicle_activation_data(df: pd.DataFrame) -> bool:
    """Update or insert vehicle activation data in Google Sheet.
    
    Args:
        df (pd.DataFrame): DataFrame containing vehicle data with columns:
            - vin: Vehicle identification number
            - Eligibility: Whether the vehicle is eligible for activation
            - Real_Activation: Current activation status
            - Activation_Error: Any error messages
            - Oem: (Optional) Vehicle manufacturer
            - Make: (Optional) Vehicle make/brand
            
    Returns:
        bool: True if all updates were successful, False otherwise
    """
    try:
        client = get_google_client()
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        
        # Get all existing data at once
        existing_data = sheet.get_all_records()
        existing_df = pd.DataFrame(existing_data)
        
        # Get headers
        headers = sheet.row_values(1)
        vin_col = headers.index("vin") + 1
        activation_col = headers.index("Activation") + 1
        evalue_col = headers.index("EValue") + 1
        real_activation_col = headers.index("Real Activation") + 1
        eligibility_col = headers.index("Eligibility") + 1
        error_col = headers.index("Activation Error") + 1
        oem_col = headers.index("Oem") + 1
        make_col = headers.index("Make") + 1
        ownership_col = headers.index("Ownership") + 1
        country_col = headers.index("Country") + 1
        account_owner_col = headers.index("account_owner") + 1
        updates = []
        inserts = []
        
        # Create a set of existing VINs for faster lookup
        existing_vins = set(existing_df['vin'].astype(str))
        
        for _, row in df.iterrows():
            vin = str(row['vin'])
            
            # Check if account_owner exists in the DataFrame columns
            account_owner_value = row.get('account_owner', "") if 'account_owner' in df.columns else ""
            
            if vin in existing_vins:
                # Find the row number in the existing data
                row_idx = existing_df[existing_df['vin'] == vin].index[0] + 2  # +2 because of 0-based index and header row
                
                # Update existing row
                updates.append({
                    'range': f'R{row_idx}C{real_activation_col}',
                    'values': [[(row['Real_Activation'])]]
                })
                updates.append({
                    'range': f'R{row_idx}C{eligibility_col}',
                    'values': [[row['Eligibility']]]
                })
                updates.append({
                    'range': f'R{row_idx}C{error_col}',
                    'values': [[row['Activation_Error'] if row['Activation_Error'] is not None else '']]
                })
                updates.append({
                    'range': f'R{row_idx}C{account_owner_col}',
                    'values': [[account_owner_value]]
                })
            else:
                new_row = [""] * len(headers)  # Create empty row with same length as headers
                new_row[vin_col - 1] = vin
                new_row[activation_col - 1] = False
                new_row[evalue_col - 1] = False
                new_row[real_activation_col - 1] = False
                new_row[eligibility_col - 1] = True
                new_row[error_col - 1] = row['Activation_Error']
                new_row[oem_col - 1] = 'TESLA'
                new_row[make_col - 1] = 'TESLA'
                new_row[ownership_col - 1] = "Bib"
                new_row[country_col - 1] = 'France'
                new_row[account_owner_col - 1] = account_owner_value
                inserts.append(new_row)
        
        # Execute batch updates
        if updates:
            sheet.batch_update(updates)
            
        # Insert new rows
        if inserts:
            sheet.append_rows(inserts)
            
        return True
        
    except Exception as e:
        logging.error(f"Error updating vehicle activation data in Google Sheet: {str(e)}")
        return False 
