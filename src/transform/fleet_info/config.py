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
COL_DTYPES = {
    "end_of_contract_date": "datetime64[ns]",
    "fleet_name": "category",
    "vin": "category",
    "start_date": "datetime64[ns]",
    "model": "category",
    "version": "category",
    "capacity": "float",
    "net_capacity": "float",
    "range": "float",
    "tesla_code": "category",
    "make": "category",
    "region_name": "category",
    "activation_status": "bool",
}

