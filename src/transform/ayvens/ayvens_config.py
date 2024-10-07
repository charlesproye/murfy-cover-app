from os import path

AYVENS_FLEET_INFO_CSV = path.join(path.dirname(__file__), "data_cache/fleet_info.csv")
AYVENS_FLEET_INFO_PARQUET = path.join(path.dirname(__file__), "data_cache/fleet_info.parquet")
AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE = path.join(path.dirname(__file__), "data_cache/fleet_info - Global NL.csv")
AYVENS_FLEET_WITH_CAR_REGISTRATION = path.join(path.dirname(__file__), "data_cache/fleet_info - Global NL 2.csv")

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
