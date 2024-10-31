# High mobility 
HIGH_MOBILITY_BRANDS = [
    "kia",
    "mercedes-benz",
    "ford",
    "renault",
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

KEYS = [
    "slope",
    "intercept",
    "r_value",
    "p_value",
    "std_err",
]

RENAME_COLS_DICT:dict[str, str] = {
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
    "charging.battery_level": "battery_level",
}

COLS_TO_KEEP = [
    "date",
    "soc",
    "odometer",
    "estimated_range",
    "battery_energy",
    "soc",
    "vin",
]

COL_DTYPES = {
    "date": "datetime64[s]",
    "soc": "float",
    "odometer": "float",
    "estimated_range": "float",
    "battery_energy": "float",
    "soc": "float",
    "vin": "string",
    "capacity": "float",
}

# BMW 
COL_DTYPES_BMW = {
    "charging_ac_ampere": "float",
    "charging_ac_voltage": "float",
    "charging_method": "category",
    "charging_plug_connected": "category",
    "charging_status": "category",
    "coolant_temperature": "float",
    "kombi_remaining_electric_range": "float",
    "mileage": "float",
    "soc_customer_target": "float",
    "soc_hv_header": "float",
    "soc_target_charging_time_forecast": "float",
    "teleservice_status": "category",
    "vin": "string",
}
