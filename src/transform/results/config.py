from core.pandas_utils import *

UPDATE_FREQUENCY = pd.Timedelta(days=7)

VALID_SOH_POINTS_LINE_BOUNDS = DF({
  "odometer": [20_000, 200_000, 0, 200_000],
  "soh": [1.0, 0.95, 0.9, 0.5],
  "point": ["A", "B", "A", "B"],
  "bound": ["max", "max", "min", "min"]
}).set_index(["bound", "point"])

MAKES_WITHOUT_SOH = [
  'bmw',
  'kia',
]

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

