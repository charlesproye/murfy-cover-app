TABLE_QUERY = """
SELECT * FROM vehicle
JOIN vehicle_model ON vehicle.vehicle_model_id = vehicle_model.id
JOIN oem ON vehicle_model.oem_id = oem.id
"""
COLS_NAME_MAPPING = {
    "fleet_name": "fleet",
    "model_name": "model",
    "oem_name": "make",
    "region_name": "region",
    "type": "version",
    "version": "tesla_code",
    "autonomy": "range",
}
COLS_TO_KEEP = [
    "end_of_contract_date",
    "vin",
    "start_date",
    "model",
    "version",
    "capacity",
    "net_capacity",
    "range",
    "tesla_code",
    "make",
]
COL_DTYPES = {
    "end_of_contract_date": "datetime64[ns]",
    "vin": "string",
    "start_date": "datetime64[ns]",
    "model": "string",
    "version": "string",
    "capacity": "float",
    "net_capacity": "float",
    "range": "float",
    "tesla_code": "string",
    "make": "string",
}
