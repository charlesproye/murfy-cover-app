TABLE_QUERY = """
SELECT * FROM vehicle
JOIN vehicle_model ON vehicle.vehicle_model_id = vehicle_model.id
JOIN oem ON vehicle_model.oem_id = oem.id
JOIN fleet ON vehicle.fleet_id = fleet.id
JOIN region ON vehicle.region_id = region.id
"""
COLS_NAME_MAPPING = {
    "model_name": "model",
    "oem_name": "make",
    "type": "version",
    "version": "tesla_code",
    "autonomy": "range",
}
COLS_TO_KEEP = [
    "end_of_contract_date",
    "fleet_name",
    "vin",
    "start_date",
    "model",
    "version",
    "capacity",
    "net_capacity",
    "range",
    "tesla_code",
    "make",
    "region_name",
]
COL_DTYPES = {
    "end_of_contract_date": "datetime64[ns]",
    "fleet_name": "string",
    "vin": "string",
    "start_date": "datetime64[ns]",
    "model": "string",
    "version": "string",
    "capacity": "float",
    "net_capacity": "float",
    "range": "float",
    "tesla_code": "string",
    "make": "string",
    "region_name": "string",
}

