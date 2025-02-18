import pandas as pd
from typing import Tuple

def validate_vehicle_data(vehicle: pd.Series) -> Tuple[bool, str]:
    """Validate vehicle data before processing.
    
    Args:
        vehicle (pd.Series): Vehicle data to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    required_fields = ['vin', 'make', 'model', 'oem', 'owner', 'country']
    
    for field in required_fields:
        if field not in vehicle or pd.isna(vehicle[field]):
            return False, f"Missing required field: {field}"
            
    if not isinstance(vehicle['vin'], str) or len(vehicle['vin']) < 10:
        return False, f"Invalid VIN format: {vehicle['vin']}"
        
    string_fields = ['make', 'model', 'oem', 'owner', 'country', 'licence_plate']
    for field in string_fields:
        if field in vehicle and not pd.isna(vehicle[field]):
            if not isinstance(vehicle[field], str):
                return False, f"Field {field} must be a string"
                
    return True, "" 
