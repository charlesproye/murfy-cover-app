from core.pandas_utils import *


UPDATE_FREQUENCY = pd.Timedelta(days=7)

MAKES = {
    "tesla",
    "mercedes-benz",
    "bmw",
    "kia",
    "renault",
    "volvo",
    "stellantis",
    "ford",
    "tesla-fleet-telemetry"
}

VALID_SOH_POINTS_LINE_BOUNDS = DF({
  "odometer": [20_000, 200_000, 0, 200_000],
  "soh": [1.0, 0.95, 0.9, 0.5],
  "point": ["A", "B", "A", "B"],
  "bound": ["max", "max", "min", "min"]
}).set_index(["bound", "point"])

VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS = {
    "soh": "soh",
    "odometer": "odometer",
    "level_1": "level_1",
    "level_2": "level_2",
    "level_3": "level_3",
    "vehicle_id": "vehicle_id",
    "date":"timestamp"
}

# Eval strings to filter raw results SoH per brand 
# Use eval/where instead of query because we don't want to remove lines containing other results (such as odometer/charging levels).
SOH_FILTER_EVAL_STRINGS: dict[callable] = {
    "tesla": "soh = soh.where(soc_diff > 40 & soh.between(0.75, 1.05))",
    "volvo": "soh = soh.where(soc > 0.7)",
    "renault": "soh = soh.where(soc > 0.5)",
    "ford": "soh = soh",
    "mercedes-benz": "soh = soh",
    "bmw": "soh = soh",
    "kia": "soh = soh",
    "stellantis": "soh = soh",
    "tesla-fleet-telemetry": "soh = soh.where(soc_diff > 5 & soh.between(0.91, 1.1))",
}

