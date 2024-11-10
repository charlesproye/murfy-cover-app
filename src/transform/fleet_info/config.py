# DB
RDB_TABLES_MERGE_KWARGS = {
    "vehicle_model": dict(left_on=["model", "version"], right_on=["model_name", "type"], src_dest_cols={"id": "vehicle_model_id"}),
    "region": dict(left_on="country", right_on="region_name", src_dest_cols={"id": "region_id"}),
    "fleet": dict(left_on="fleet", right_on="fleet_name", src_dest_cols={"id": "fleet_id"}),
}

#S3
S3_JSON_FLEET_INFO_RESPONSE_KEY ="fleet_info/tesla/raw_fleet_info.json"
S3_INITIAL_FLEET_INFO_KEY = "fleet_info/tesla/initial_fleet_info.parquet"

# Ayvens
AYVENS_FLEET_INFO_CSV_KEY = "fleet_info/ayvens/fleet_info_with_regions.csv"
AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE_KEY = "fleet_info/ayvens/fleet_info - Global NL.csv"
AYVENS_FLEET_WITH_CAR_REGISTRATION_KEY = "fleet_info/ayvens/fleet_info - Global NL 2.csv"
AYVENS_FLEET_INFO_PARQUET_KEY = "fleet_info/ayvens/fleet_info.parquet"
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
    "type": "version"
}
MODEL_VERSION_NAME_MAPPING = {
    "R90 Life (batterijkoop) 5d": "R90",
    "R135 Edition One (batterijkoop) 5d": "R135",
    "R135 Intens (batterijkoop) 5d": "R135",
    "R135": "R135",
}
MAKE_NAME_MAPPING = {
    # bmw
    "bmw": "BMW",
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
AYVENS_COL_DTYPES = {
    "licence plate": "string",
    "make": "string",
    "model": "string",
    "version": "string",
    # "capacity": "float",
    # "autonomie": "float",
    # "end of contract": "object",
    "country": "string",
    "category": "string",
    "activated": "string",
    "ownership": "string",
    # "registration_date": "object",
    # "contract_start_date": "object",
    "vin": "string",
}
