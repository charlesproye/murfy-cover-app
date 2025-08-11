LEVEL_1_MAX_POWER = 8 #this is to approximate the power of the charger that can go up to 7 kW
LEVEL_2_MAX_POWER = 45 #this is to approximate the power of the charger that can go up to 44 kW

PROCESSED_PHASES_CACHE_KEY_TEMPLATE = "processed_phases/processed_phases_{make}.parquet"

ODOMETER_MILES_TO_KM = {
    "tesla": 1.60934,
    "tesla-fleet-telemetry": 1.60934,
}

# Coefficient to scale soc to 0-100%
SCALE_SOC = {
    'tesla-fleet-telemetry': 1,
    'mercedes-benz': 100,
    'volvo-cars': 100,
    'kia': 100,
    'renault': 100,
    'ford': 100,
    'stellantis': 100,
    'bmw': 1,
    'volkswagen': 1,
}


# Minimum threshold to consider a charging or discharging phase
SOC_DIFF_THRESHOLD = {
    'tesla-fleet-telemetry': 0.05,
    'mercedes-benz': 0.05,
    'volvo-cars': 0.05,
    'kia': 0.05,
    'renault': 0.05,
    'ford': 0.05,
    'stellantis': 0.05,
    'bmw': 0.05,
    'volkswagen': 0.05,
}
