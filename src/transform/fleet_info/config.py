#S3
S3_JSON_FLEET_INFO_RESPONSE_KEY ="fleet_info/tesla/raw_fleet_info.json"
S3_INITIAL_FLEET_INFO_KEY = "fleet_info/tesla/initial_fleet_info.parquet"

# Global
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

# Ayvens
AYVENS_FLEET_INFO_CSV_KEY = "fleet_info/ayvens/fleet_info_with_regions.csv"
AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE_KEY = "fleet_info/ayvens/fleet_info - Global NL.csv"
AYVENS_FLEET_WITH_CAR_REGISTRATION_KEY = "fleet_info/ayvens/fleet_info - Global NL 2.csv"
AYVENS_FLEET_INFO_PARQUET_KEY = "fleet_info/ayvens/fleet_info.parquet"

FLEET_INFO_COLS_NAME_MAPPING = {
    "type": "version"
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
