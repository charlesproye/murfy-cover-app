import pandas as pd

from core.constants import *

# Variables name standardization
COLS_NAME_DICTS = {
    "diagnostics.odometer": "odometer",
    "charging.battery_level": "charging_soc",
    "diagnostics.battery_level": "diagnostics_soc",
    "charging.status": "charging_status",
    "charging.estimated_range": "estimated_range",
    "charging.charging_rate": "charging_power",
}
# Unit standar dization
UNIT_CONVERSION_OPS = {
    "kilometers": lambda col: col,
    "kilowatts": lambda col: col,
    "miles": lambda col: col * MILES_TO_KM,
    "minutes": lambda col: pd.to_timedelta(col, "minutes")
}
# Battery state from charging status variable
CHARGING_STATUS_TO_CHARGING_VAL = ['charging']
CHARGING_STATUS_TO_DISCHARGING_VAL = [
  'cable_unplugged',
  'charging_complete',
  'charging_paused',
  'not_charging',
  'charging_error',
  'flap_open',
]
#{nan: 1708, 'cable_unplugged': 278, 'charging': 34, 'charging_complete': 61, 'charging_paused': 8, 'not_charging': 80, 'charging_error': 3, 'flap_open': 1}
