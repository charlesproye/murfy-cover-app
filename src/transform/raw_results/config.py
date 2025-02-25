LEVEL_1_MAX_POWER = 8 #this is to approximate the power of the charger that can go up to 7 kW
LEVEL_2_MAX_POWER = 45 #this is to approximate the power of the charger that can go up to 44 kW

MERCEDES_SOH_MODEL_CALCULATIONS:dict[str,str] = {
    'vito': "estimated_range / soc / range / 0.97",
    'sprinter': "estimated_range / soc / range / 0.92",
    'default': "estimated_range / soc / range",
}

RAW_RESULTS_CACHE_KEY_TEMPLATE = "raw_results/{make}.parquet"
