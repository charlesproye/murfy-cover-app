LEVEL_1_MAX_POWER = 2.5
LEVEL_2_MAX_POWER = 19.5

MERCEDES_SOH_MODEL_CALCULATIONS:dict[str,str] = {
    'vito': "estimated_range / soc / range / 0.97",
    'sprinter': "estimated_range / soc / range / 0.92",
    'default': "estimated_range / soc / range",
}

VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS = {
    "soh": "soh",
    "odometer": "odometer",
    "level_1": "level_1",
    "level_2": "level_2",
    "level_3": "level_3",
    "vehicle_id": "vehicle_id",
    "date":"timestamp"
}

