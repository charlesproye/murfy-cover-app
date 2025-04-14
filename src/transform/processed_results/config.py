from core.pandas_utils import *
from transform.raw_results.odometer_aggregation import agg_last_odometer
from transform.raw_results.ford_results import get_results as get_ford_results
from transform.raw_results.tesla_results import get_results as get_tesla_results
from transform.raw_results.volvo_results import get_results as get_volvo_results
from transform.raw_results.renault_results import get_results as get_renault_results
from transform.raw_results.mercedes_results import get_results as get_mercedes_results
from transform.raw_results.stellantis_results import get_results as get_stellantis_results
from transform.raw_results.tesla_fleet_telemetry import get_results as get_tesla_fleet_telemetry


UPDATE_FREQUENCY = pd.Timedelta(days=7)

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


GET_RESULTS_FUNCS = {
    "tesla": get_tesla_results,
    "mercedes-benz": get_mercedes_results,
    "bmw": lambda: agg_last_odometer("bmw"),
    "kia": lambda: agg_last_odometer("kia"),
    "renault": get_renault_results,
    "volvo": get_volvo_results,
    "stellantis": get_stellantis_results,
    "ford": get_ford_results,
    "tesla-fleet-telemetry": get_tesla_fleet_telemetry
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
    "tesla-fleet-telemetry": "soh = soh.where(soc_diff > 40 & soh.between(0.75, 1.05))",
}

