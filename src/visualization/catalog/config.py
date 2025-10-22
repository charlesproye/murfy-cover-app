# Étapes possibles
STEPS = {
    "raw_ts": "raw_ts/{oem}/time_series/raw_ts_spark.parquet/",
    "processed_phases": "processed_phases/processed_phases_{oem}.parquet/",
    "result_phases": "result_phases/result_phases_{oem}.parquet/",
}

# Liste OEMs disponibles (à extraire dynamiquement ou mettre en dur)
OEM_LIST = [
    "renault",
    "stellantis",
    "bmw",
    "volvo-cars",
    "volkswagen",
    "ford",
    "kia",
    "mercedes-benz",
    "tesla-fleet-telemetry",
]

