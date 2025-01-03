# DB
RIGHT_MERGES_RDB_TABLES_FLEET_INFO_DEST_COLS = [
    "fleet_id",
    "region_id",
    "vehicle_model_id",
    "licence_plate",
    "end_of_contract_date",
    "id",
    "activation_status",
    "vin",
]
LEFT_MERGES_RDB_TABLES_FLEET_INFO_KWARGS = [
    dict(rhs="vehicle_model",left_on=["model", "version"], right_on=["model_name", "type"], src_dest_cols={"id": "vehicle_model_id"}),
    dict(rhs="region", left_on="country", right_on="region_name", src_dest_cols={"id": "region_id"}),
    dict(rhs="fleet", left_on="fleet", right_on="fleet_name", src_dest_cols={"id": "fleet_id"}),
]
COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE = [
    "fleet_id", #
    "region_id", #
    "vehicle_model_id", #
    "purchase_date",
    "licence_plate", #
    "end_of_contract_date", #
    "id",
    #"updated_at",
    #"created_at",
    "activation_status", #
    "vin", #
]

#S3
# TEST_TESLA_FLEET_INFO_KEY = "fleet_info/tesla/raw_fleet_info.json"
# S3_JSON_FLEET_INFO_RESPONSE_KEYS = [
#     "fleet_info/tesla/raw_fleet_info.json",
#     "fleet_info/tesla/ayvens-blbv.json",
#     "fleet_info/tesla/ayvens-nv.json",
#     "fleet_info/tesla/ayvens-nva.json",
#     "fleet_info/tesla/ayvens-slbv.json",
#     "fleet_info/tesla/ayvens.json",
# ]
# All
FLEET_INFO_KEY = "fleet_info/fleet_info.csv"

# Ayvens
# AYVENS_FLEET_INFO_CSV_KEY = "fleet_info/ayvens/fleet_info_with_regions.csv"
# AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE_KEY = "fleet_info/ayvens/fleet_info - Global NL.csv"
# AYVENS_FLEET_WITH_CAR_REGISTRATION_KEY = "fleet_info/ayvens/fleet_info - Global NL 2.csv"
# AYVENS_FLEET_INFO_PARQUET_KEY = "fleet_info/ayvens/fleet_info.parquet"

RENAME_AYVENS_COLS = {
    "end of contract": "end_of_contract_date",
    "activated": "activation_status"
}
COLS_TO_MERGE_ON_AYVENS = {
    "Contract start datum": "contract_start_date",
    "Car registration date": "registration_date",
    "Contract start date": "contract_start_date"
}
FLEET_INFO_COLS_NAME_MAPPING = {
    "end_of_contract": "end_of_contract_date",
    "activated": "activation_status",
    "type": "version",
    "ownership_": "owner",
}
MODEL_VERSION_NAME_MAPPING = {
    "R90 Life (batterijkoop) 5d": "R90",
    "R135 Edition One (batterijkoop) 5d": "R135",
    "R135 Intens (batterijkoop) 5d": "R135",
    "R135": "R135",
}
MAKE_NAME_MAPPING = {
    # bmw
    "BMW": "bmw",
    # high mobility
    "FORD": "ford",
    "Mercedes-Benz": "mercedes-benz",
    "MERCEDES-BENZ": "mercedes-benz",
    "Mercedes": "mercedes-benz",
    "KIA": "kia",
    "Kia": "kia",
    # stellantis
    "PEUGEOT": "peugeot",
    "Peugeot": "peugeot",
    "OPEL": "opel",
    "Opel": "opel",
    "FIAT": "fiat",
    "Fiat": "fiat",
    "Renault": "renault",
    "RENAULT": "renault",
    "TESLA": "tesla",
    "Tesla": "tesla",
    "Citroën": "citroën",
    "DS": "ds",
}
COUNTRY_NAME_MAPPING = {
    "NL": "netherlands"   
}
COL_DTYPES = {
    "licence plate": "string",
    "make": "string",
    "model": "string",
    "version": "string",
    # "capacity": "float",
    # "autonomie": "float",
    "end of contract": "datetime64[ns]",
    "end_of_contract_date": "datetime64[ns]",
    "country": "string",
    "category": "string",
    "activated": "string",
    "owner": "string",
    # "registration_date": "object",
    "contract_start_date": "datetime64[ns]",
    "vin": "string",
    "activation_status": "string",
}

