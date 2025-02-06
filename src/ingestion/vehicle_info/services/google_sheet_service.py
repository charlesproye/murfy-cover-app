import logging
import time
from typing import Optional
from ..config.settings import SPREADSHEET_ID
from ..utils.google_sheets_utils import get_google_client
from gspread.exceptions import APIError

# Rate limiting settings
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 32  # Maximum delay between retries in seconds
REQUESTS_PER_MINUTE = 50  # Conservative limit to avoid quota issues
MIN_REQUEST_INTERVAL = 60.0 / REQUESTS_PER_MINUTE  # Minimum time between requests

# Keep track of last request time
last_request_time = 0

def wait_for_rate_limit():
    """Wait if necessary to respect rate limits."""
    global last_request_time
    current_time = time.time()
    time_since_last_request = current_time - last_request_time
    
    if time_since_last_request < MIN_REQUEST_INTERVAL:
        sleep_time = MIN_REQUEST_INTERVAL - time_since_last_request
        logging.debug(f"Rate limiting: waiting {sleep_time:.2f} seconds")
        time.sleep(sleep_time)
    
    last_request_time = time.time()

async def update_google_sheet_status(vin: str, real_activation: Optional[bool], error_message: Optional[str] = None) -> bool:
    """Update Real Activation and Activation Error columns in Google Sheet.
    
    Args:
        vin (str): Vehicle VIN
        real_activation (Optional[bool]): Actual activation status, None means don't update
        error_message (Optional[str]): Error message if any
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    delay = INITIAL_RETRY_DELAY
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        try:
            # Wait for rate limit before making request
            wait_for_rate_limit()
            
            client = get_google_client()
            sheet = client.open_by_key(SPREADSHEET_ID).sheet1
            
            # Find the row with the VIN
            cell = sheet.find(vin)
            if not cell:
                logging.error(f"VIN {vin} not found in Google Sheet")
                return False
                
            # Get headers to find column indices
            headers = sheet.row_values(1)
            real_activation_col = headers.index("Real Activation") + 1
            error_col = headers.index("Activation Error") + 1
            
            # Prepare batch update
            updates = []
            
            # Update Real Activation status only if a value is provided
            if real_activation is not None:
                updates.append({
                    'range': f'R{cell.row}C{real_activation_col}',
                    'values': [[str(real_activation).upper()]]
                })
            
            # Update error message
            updates.append({
                'range': f'R{cell.row}C{error_col}',
                'values': [[error_message or ""]]
            })
            
            # Execute batch update
            if updates:
                sheet.batch_update(updates)
            
            logging.info(f"Updated Google Sheet for VIN {vin}: Real Activation={real_activation if real_activation is not None else 'unchanged'}, Error={error_message or ''}")
            return True
            
        except APIError as e:
            last_error = e
            if e.response.status_code == 429:  # Rate limit exceeded
                sleep_time = min(delay * (2 ** attempt), MAX_RETRY_DELAY)
                logging.warning(f"Rate limit hit, waiting {sleep_time} seconds before retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(sleep_time)
                continue
            logging.error(f"API Error updating Google Sheet for VIN {vin}: {str(e)}")
            return False
            
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                sleep_time = min(delay * (2 ** attempt), MAX_RETRY_DELAY)
                logging.warning(f"Error updating Google Sheet, retrying in {sleep_time} seconds: {str(e)}")
                time.sleep(sleep_time)
                continue
            logging.error(f"Failed to update Google Sheet after {MAX_RETRIES} attempts. Last error: {str(last_error)}")
            return False
            
    return False 
