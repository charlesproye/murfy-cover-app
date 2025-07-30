

MAKES_WITHOUT_SOH = [
    "tesla-fleet-telemetry",
    "bmw",
    "mercedes-benz",
    "ford",
    "volvo-cars",
    "stellantis",
    "kia",
    "renault",
]


LEVEL_1_MAX_POWER = 8 # This is to approximate the power of the charger that can go up to 7 kW
LEVEL_2_MAX_POWER = 45 # This is to approximate the power of the charger that can go up to 44 kW



RAW_RESULTS_CACHE_KEY_TEMPLATE = "raw_results/raw_results_{make}_spark.parquet"
