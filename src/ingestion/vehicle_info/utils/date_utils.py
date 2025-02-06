import logging
import pandas as pd
from typing import Optional

def convert_date_format(date_str) -> Optional[str]:
    """Convert different date formats to YYYY-MM-DD format.
    
    Args:
        date_str: The date string to convert
        
    Returns:
        str: Formatted date string in YYYY-MM-DD format or None if conversion fails
    """
    if pd.isna(date_str):
        return None
        
    try:
        # Handle pandas Timestamp
        if isinstance(date_str, pd.Timestamp):
            return date_str.strftime('%Y-%m-%d')
            
        # Handle string formats
        date_str = str(date_str).split()[0]  # Take only the date part
        
        if '.' in date_str:  # DD.MM.YYYY format
            day, month, year = date_str.split('.')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
        elif '/' in date_str:  # MM/DD/YYYY format
            month, day, year = date_str.split('/')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
        elif '-' in date_str:  # DD-MM-YYYY or YYYY-MM-DD format
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # Already YYYY-MM-DD
                return date_str
            else:  # DD-MM-YYYY
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                
        else:
            logging.warning(f"Unrecognized date format: {date_str}")
            return None
            
    except Exception as e:
        logging.warning(f"Invalid date format: {date_str}, error: {str(e)}")
        return None 
