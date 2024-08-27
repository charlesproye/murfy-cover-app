import pandas as pd

from core.constants import *

COLS_NAME_DICTS = {
    "diagnostics.odometer": "odometer",
    "charging.battery_level": "raw_soc",
    "charging.status": "charging_status",
    "charging.estimated_range": "estimated_range",
    "charging.max_range": "charging_max_range",
    "charging.starter_battery_state": "charging_starter_battery_state",
    "charging.charging_rate.kilowatts": "charging_power",
    "charging.max_range": "charging_max_range",
    "charging.battery_level_at_departure": "raw_soc_at_end_of_charge",
    "charging.preconditioning_departure_status": "charging_preconditioning_departure_status",
    "charging.preconditioning_remaining_time": "charging_preconditioning_remaining_time",
    "charging.smart_charging_status": "smart_charging_status",
}

UNIT_CONVERSION_OPS = {
    "kilometers": lambda col: col,
    "kilowatts": lambda col: col,
    "miles": lambda col: col * MILES_TO_KM,
    "minutes": lambda col: pd.to_timedelta(col, "minutes")
}
