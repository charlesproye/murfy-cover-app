import os
import pandas as pd
from logging import getLogger
import asyncio
from typing import Optional, Dict, Any, List
import time
from gspread.exceptions import APIError
from gspread import Cell

from core.pandas_utils import *
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.config import *
from config import *
from .utils.google_sheets_utils import get_google_client

logger = getLogger("ingestion.vehicle_info")

SPREADSHEET_ID = "1zGwSY41eN00YQbaNf9HNk3g5g6KQaAD1FY7-XS8Uf9w"
CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH", "credentials.json")

# Rate limiting settings
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 32  # Maximum delay between retries in seconds

# Constants and Mappings
OEM_NAME_MAPPING = {
    'Mercedes': 'mercedes',
    'MERCEDES': 'mercedes',
    'mercedes-benz': 'mercedes',
    'Mercedes-Benz': 'mercedes',
    'MERCEDES-BENZ': 'mercedes',
    'Mercedes': 'mercedes',
    'Stellantis': 'stellantis',
    'STELLANTIS': 'stellantis',
    'stellantis': 'stellantis'
}

COUNTRY_NAME_MAPPING = {
    'NL': 'Netherlands',
    'FR': 'France',
    'BE': 'Belgium',
    'DE': 'Germany',
    'LU': 'Luxembourg',
    'ES': 'Spain',
    'IT': 'Italy',
    'PT': 'Portugal',
    'GB': 'United Kingdom',
    'UK': 'United Kingdom'
}

# Column data types
COL_DTYPES = {
    'vin': str,
    'licence_plate': str,
    'make': str,
    'model': str,
    'type': str,
    'oem': str,
    'owner': str,
    'country': str,
    'activation': bool,
    'real_activation': bool
}

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
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
                
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
        df = df.pipe(map_col_to_dict, "country", COUNTRY_NAME_MAPPING)
        
        # Map OEM names
        if 'oem' in df.columns:
            df['oem'] = df['oem'].apply(lambda x: OEM_NAME_MAPPING.get(x, x.lower()) if pd.notna(x) else x)
        
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
        
        logger.info(f"Successfully processed fleet info. Final shape: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading fleet info: {str(e)}")
        raise

if __name__ == "__main__":
    df = asyncio.run(read_fleet_info())
    print(df)
    
