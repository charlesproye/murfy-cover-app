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
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.config import *
from ingestion.vehicle_info.config.credentials import SPREADSHEET_ID 
from ingestion.vehicle_info.config.mappings import OEM_MAPPING, COUNTRY_MAPPING, COL_DTYPES
from ingestion.vehicle_info.config.settings import MAX_RETRIES, INITIAL_RETRY_DELAY, MAX_RETRY_DELAY
from ingestion.vehicle_info.utils.google_sheets_utils import get_google_client
# from config import COL_DTYPES

logger = getLogger("ingestion.vehicle_info")
 

def get_google_sheet_data(max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY) -> List[Dict]:
    """Récupère les données de la Google Sheet avec gestion des rate limits et des erreurs."""
    client = get_google_client()
    delay = initial_delay
    last_error = None
    
    for attempt in range(max_retries):
        try:
            sheet = client.open_by_key(SPREADSHEET_ID).sheet1
            data = sheet.get_all_records()
            logger.info(f"Successfully fetched {len(data)} rows from Google Sheets")
            return data
            
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

def safe_astype(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
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
                    df[col] = df[col].fillna(False)
                    df[col] = df[col].map({'TRUE': True, 'True': True, True: True,
                                         'FALSE': False, 'False': False, False: False})
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

async def read_fleet_info(owner_filter: Optional[str] = None) -> pd.DataFrame:
    """Read fleet information from Google Sheets."""
    try:
        logger.info("Starting to read fleet information...")
        # Get data with retries
        data = get_google_sheet_data()
        df = pd.DataFrame(data)
        
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
        df = df.pipe(map_col_to_dict, "country", COUNTRY_MAPPING)
        
        # Map OEM names
        if 'oem' in df.columns:
            df['oem'] = df['oem'].apply(lambda x: OEM_MAPPING.get(x, x.lower()) if pd.notna(x) else x)
        
        for col in ['make', 'model', 'type']:
            if col in df.columns:
                df[col] = df[col].str.lower()
        
        # Convert dates with explicit format handling
        date_columns = ['start_date', 'end_of_contract']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', dayfirst=True, errors='coerce')
        
        # Remove duplicates and convert types
        df = df.drop_duplicates(subset="vin")
        
        # Ensure all required columns exist before type conversion
        for col in COL_DTYPES:
            if col not in df.columns:
                logger.warning(f"Missing column {col}, adding empty column")
                df[col] = None
                
        df = df.pipe(safe_astype, COL_DTYPES)
        df = df.pipe(clean_version)
        df = df.pipe(format_licence_plate)
        
        logger.info(f"Successfully processed fleet info. Final shape: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading fleet info: {str(e)}")
        raise

if __name__ == "__main__":
    df = asyncio.run(read_fleet_info())
    print(df)
    
