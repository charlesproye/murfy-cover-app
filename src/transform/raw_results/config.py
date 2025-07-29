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


LEVEL_1_MAX_POWER = 8 #this is to approximate the power of the charger that can go up to 7 kW
LEVEL_2_MAX_POWER = 45 #this is to approximate the power of the charger that can go up to 44 kW

MERCEDES_SOH_MODEL_CALCULATIONS:dict[str,str] = {
    'vito': "estimated_range / soc / range / 0.97",
    'sprinter': "estimated_range / soc / range / 0.92",
    'default': "estimated_range / soc / range",
}

RAW_RESULTS_CACHE_KEY_TEMPLATE = "raw_results/{make}_spark.parquet"

TESLA_USE_COLS = [
    "vin",
    "trimmed_in_charge_idx",
    "trimmed_in_charge",
    "charge_energy_added",
    "soc",
    "inside_temp",
    "capacity",
    "net_capacity",
    "range",
    "odometer",
    "model",
    "date",
    "tesla_code",
    "battery_heater",
    "charging_power",
    "version",
]
