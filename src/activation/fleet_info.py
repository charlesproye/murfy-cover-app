import os
import pandas as pd
from logging import getLogger
import asyncio
from typing import Optional, Dict, Any, List
import time
from gspread.exceptions import APIError
from gspread import Cell
import re


from core.pandas_utils import *
from core.config import *
from core.gsheet_utils import get_google_client
from .config.credentials import SPREADSHEET_ID 
from .config.mappings import OEM_MAPPING, COUNTRY_MAPPING, COL_DTYPES, suffixes_to_remove, mappings
from .config.settings import MAX_RETRIES, INITIAL_RETRY_DELAY, MAX_RETRY_DELAY


logger = getLogger("ingestion.vehicle_info")
 

def get_google_sheet_data(max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY) -> List[Dict]:
    """Récupère les données de la Google Sheet avec gestion des rate limits et des erreurs."""
    client = get_google_client()
    delay = initial_delay

    last_error = None
    
    for attempt in range(max_retries):
        try:
            sheet = client.open_by_key(SPREADSHEET_ID).sheet1

            # Get all values including headers
            data = sheet.get_all_records()
            
            # Get headers from first row and use them as column names
            df = pd.DataFrame(data)
            
            logger.info(f"Successfully fetched {len(df)} rows from Google Sheets")
            return df
        
        except APIError as e:
            last_error = e
            if e.response.status_code in [429, 500, 503]:  # Rate limit or server error
                sleep_time = min(delay * (2 ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"API error (status {e.response.status_code}), attempt {attempt + 1}/{max_retries}. "
                                f"Waiting {sleep_time} seconds before retry...")
                time.sleep(sleep_time)
                continue
            raise  # Re-raise if it's not a retryable error

        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                sleep_time = min(delay * (2 ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"Unexpected error: {str(e)}, attempt {attempt + 1}/{max_retries}. "
                            f"Waiting {sleep_time} seconds before retry...")
                time.sleep(sleep_time)
                continue
            logger.error(f"Failed to fetch data after {max_retries} attempts. Last error: {str(last_error)}")
            raise

def safe_astype_activation(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
    """Safely convert DataFrame columns to specified types.
    
    Args:
        df: DataFrame to convert
        dtypes: Dictionary of column names and their types
        
    Returns:
        DataFrame with converted types
    """
    df = df.copy()
    for col, dtype in dtypes.items():
        if col in df.columns:
            try:
                if dtype == bool:
                    # First replace all empty values with False
                    df[col] = df[col].fillna(False)
                    # Then map the remaining values
                    df[col] = df[col].map({'TRUE': True, 'True': True, True: True,
                                         'FALSE': False, 'False': False, False: False, '': False})
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                logging.warning(f"Could not convert column {col} to {dtype}: {str(e)}")
    return df

def map_col_to_dict(df: pd.DataFrame, col: str, mapping: Dict[str, str]) -> pd.DataFrame:
    """Map values in a column using a dictionary.
    
    Args:
        df: DataFrame to process
        col: Column name to map
        mapping: Dictionary with mappings
        
    Returns:
        DataFrame with mapped values
    """
    df = df.copy()
    if col in df.columns:
        df[col] = df[col].str.lower().map(lambda x: mapping.get(x, x) if pd.notna(x) else x)
    return df

def clean_version(df, model_col='model', version_col='type'):
    """Clean version strings by removing model name if it appears in the version.
    
    Args:
        df: DataFrame containing model and version columns
        model_col: Name of the column containing model information
        version_col: Name of the column containing version information
        
    Returns:
        DataFrame with cleaned version strings
    """
    return df.assign(**{version_col: lambda df: df.apply(lambda row: row[version_col].replace(row[model_col], '') 
                                                         if pd.notna(row[model_col]) and pd.notna(row[version_col]) and row[model_col] in row[version_col] 
                                                         else row[version_col], axis=1)})

def format_licence_plate(df, licence_plate_col='licence_plate'):
    """Format licence plate strings by adding dashes between letters and numbers.
    
    Args:
        df: DataFrame containing licence plate column
        licence_plate_col: Name of the column containing licence plate information
        
    Returns:
        DataFrame with formatted licence plate strings
    """
    return df.assign(**{licence_plate_col: lambda df: df[licence_plate_col].apply(lambda plate: re.sub(r"([a-zA-Z]+)(\d{3})([a-zA-Z]+)", r"\1-\2-\3", plate) if pd.notna(plate) else plate)})

def standardize_model_type(df: pd.DataFrame, oem_col='oem', model_col='model', type_col='type') -> pd.DataFrame:
    """Standardize model and type strings based on predefined mappings.
    
    Args:
        df: DataFrame containing model and type columns
        oem_col: Name of the column containing OEM/make information
        model_col: Name of the column containing model information
        type_col: Name of the column containing type/version information
        
    Returns:
        DataFrame with standardized model and type strings
    """
    df = df.copy()
    
    def _standardize_row(row):
        make = str(row[oem_col]).lower() if pd.notna(row[oem_col]) else ''
        model = str(row[model_col]).lower() if pd.notna(row[model_col]) else ''
        type_val = str(row[type_col]).lower() if pd.notna(row[type_col]) else ''
        
        if not type_val or type_val == 'x' or type_val == 'unknown':
            return pd.Series({model_col: model, type_col: None})
            
        # Remove common suffixes
        for suffix in suffixes_to_remove:
            type_val = type_val.replace(f" {suffix}", "")
            
        if make in mappings and model in mappings[make]:
            model_info = mappings[make][model]
            
            # # Apply model cleaning if specified
            # if 'model_clean' in model_info:
            #     model = model_info['model_clean'](model)
                
            # Apply type patterns
            for pattern, replacement in model_info['patterns']:
                if re.search(pattern, type_val):
                    return pd.Series({model_col: model, type_col: str(replacement).lower()})
                    
        return pd.Series({model_col: model, type_col: type_val.strip()})
    
    result = df.apply(_standardize_row, axis=1)
    df[model_col] = result[model_col]
    df[type_col] = result[type_col]
    
    return df

async def read_fleet_info(owner_filter: Optional[str] = None) -> pd.DataFrame:
    """Read fleet information from Google Sheets."""
    # try:
    logger.info("Starting to read fleet information...")
    # Get data with retries
    df = get_google_sheet_data()

    if df.empty:
        raise ValueError("No data retrieved from Google Sheets")
        
    logger.info(f"Retrieved {len(df)} rows from Google Sheets")
    
    # Clean column names
    df.columns = [col if col == "EValue" else col.lower().strip().replace(' ', '_') for col in df.columns]

    # Handle potential variations of the owner column name
    owner_column_variants = ['owner', 'ownership', 'ownership_', 'fleet_owner', 'fleet']
    owner_col = None
    for variant in owner_column_variants:
        if variant in df.columns:
            owner_col = variant
            logger.info(f"Found owner column: {variant}")
            break
            
    if owner_col is None:
        raise ValueError(f"Could not find owner column. Available columns: {df.columns.tolist()}")

    # Rename the owner column to 'owner' for consistency
    df = df.rename(columns={owner_col: 'owner'})
    
    # Apply owner filter if provided
    if owner_filter:
        logger.info(f"Filtering by owner: {owner_filter}")
        initial_count = len(df)
        df = df[df['owner'].str.lower() == owner_filter.lower()]
        logger.info(f"Found {len(df)} vehicles for owner {owner_filter} (filtered from {initial_count})")
    

    # Clean and standardize data
    # df = df.pipe(map_col_to_dict, "country", COUNTRY_MAPPING)
    
    # Map OEM names
    if 'oem' in df.columns:
        df['oem'] = df['oem'].apply(lambda x: OEM_MAPPING.get(x, x.lower()) if pd.notna(x) else x)
    
    df[['oem', 'make','model','type']] = df[['oem', 'make','model','type']].apply(lambda x: x.str.lower())
    
    # Convert dates with explicit format handling
    date_columns = ['start_date', 'end_of_contract']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], yearfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Remove duplicates and convert types
    df = df.drop_duplicates(subset="vin")
    df = df.replace({pd.NaT: None})
    
    # Ensure all required columns exist before type conversion
    for col in COL_DTYPES:
        if col not in df.columns:
            logger.warning(f"Missing column {col}, adding empty column")
            df[col] = None


    # Add this before safe_astype
    df = df.pipe(safe_astype_activation, COL_DTYPES)
    df = df.pipe(clean_version, model_col='model', version_col='type')
    df = df.pipe(format_licence_plate, licence_plate_col='licence_plate')
    df = df.pipe(standardize_model_type, oem_col='oem', model_col='model', type_col='type')
    logger.info(f"Successfully processed fleet info. Final shape: {df.shape}")
    
    return df
        
    # except Exception as e:
    #     logger.error(f"Error reading fleet info: {str(e)}")
    #     raise

if __name__ == "__main__":
    df = asyncio.run(read_fleet_info(owner_filter=''))
