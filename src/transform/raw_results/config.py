LEVEL_1_MAX_POWER = 2.5
LEVEL_2_MAX_POWER = 19.5

MERCEDES_SOH_MODEL_CALCULATIONS:dict[str,str] = {
    'vito': "estimated_range / soc / range / 0.97",
    'sprinter': "estimated_range / soc / range / 0.92",
    'default': "estimated_range / soc / range",
}

RAW_RESULTS_CACHE_KEY_TEMPLATE = "raw_results/{make}.parquet"
