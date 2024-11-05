import pandas as pd

# High mobility 
HIGH_MOBILITY_BRANDS = [
    "kia",
    "mercedes-benz",
    "ford",
    "renault",
    "opel",
    "ds",
    "fiat",
    "peugeot",
    "citroen",
]

COLS_TO_CPY_FROM_FLEET_INFO = [
    "make",
    "model",
    "version",
    "dummy_soh_maker_offset",
    "dummy_soh_model_offset",
    "dummy_soh_model_slope",
    "dummy_soh_vehicle_offset",
    "capacity",
    "vin",
]

RENAME_COLS_DICT:dict[str, str] = {
    # High mobility
    "date_of_value": "date",
    "diagnostics.odometer": "odometer",
    "odometer.value": "odometer",
    "diagnostics.odometer": "odometer",
    "mileage_km": "odometer",
    "mileage": "odometer",
    "charging.battery_energy": "battery_energy",
    "charging.estimated_range": "estimated_range",
    "charging.battery_level": "soc",
    "soc_hv_header": "soc",
    "charging.battery_energy": "battery_energy",
    # BMW
    "charging_ac_ampere": "charging_ac_current",
    "kombi_remaining_electric_range": "estimated_range",
    "mileage": "odometer", # Yes, mileage is in km no need to convert it
    "soc_hv_header": "soc",
    "capacity": "capacity_according_to_data_provider",
    "model": "model_according_to_data_provider",
    # Stellantis
    'odometer.value': "odometer",
    'engine.battery.capacity': "soc",
    'externalTemperature.value': "temperature",
}

# The keys will be used to determine what columns to keep.
COL_DTYPES = {
    # Common
    "vin": "string",
    "soc": "float32",
    "odometer": "float32",
    "estimated_range": "float32",
    # High mobility
    "date": "datetime64[s]",
    "battery_energy": "float32",
    # BMW
    "charging_ac_current": "float",
    "charging_ac_voltage": "float",
    "charging_method": "category",
    "charging_plug_connected": "bool",
    "charging_status": "string",
    "coolant_temperature": "float32",
    "kombi_remaining_electric_range": "float32",
}

CHARGING_STATUS_VAL_TO_MASK = {
    #BMW
    "CHARGINGACTIVE": True,
    "<NA>": False,
    pd.NA: False,
    "NOCHARGING": False,
    "CHARGINGENDED": False,
    "CHARGINGERROR": False,
    "INITIALIZATION": False,
    "CHARGINGPAUSED": False,
}

