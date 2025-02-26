from pandas.api.types import CategoricalDtype
from pandas import Timedelta as TD
import pandas as pd

S3_PROCESSED_TSS_KEY_FORMAT = 'processed_ts/{make}/time_series/processed_tss.parquet'

ODOMETER_MILES_TO_KM = {
    "tesla": 1.60934,
}

COLS_TO_CPY_FROM_FLEET_INFO = [
    "make",
    "model",
    "version",
    "capacity",
    "owner",
    "range",
]

RENAME_COLS_DICT:dict[str, str] = {
    # High mobility
    "date_of_value": "date",
    "diagnostics.odometer": "odometer",
    "odometer.value": "odometer",
    "mileage_km": "odometer",
    "mileage": "odometer",
    "charging.battery_energy": "battery_energy",
    "charging.estimated_range": "estimated_range",
    "charging.battery_level": "soc",
    "soc_hv_header": "soc",
    "climate.outside_temperature": "outside_temp",
    "charging.status": "charging_status",
    "charging.battery_charge_type": "charging_method",
    # Mobilisight
    "datetime": "date",
    "engine.oilTemperature": "oil_temp",
    "electricity.level.percentage": "soc",
    "engine.coolantTemperature": "coolant_temp",
    "externalTemperature": "outside_temp",
    "electricity.residualAutonomy": "estimated_range",
    "electricity.batteryCapacity": "battery_energy",
    "electricity.charging.plugged": "charging_plug_connected",
    "electricity.charging.status": "charging_status",
    "electricity.charging.remainingTime": "minutes_to_full_charge",
    "electricity.charging.mode": "charging_method",
    "electricity.charging.planned": "charging_planned",
    "electricity.charging.rate": "charging_rate",
    "electricity.engineSpeed": "engine_speed",
    "electricity.battery.stateOfHealth": "soh_oem",
    # Mercedes
    "charging.max_range": "max_range",
    "charging.charging_rate": "charging_rate",
    # BMW
    "charging_ac_ampere": "charging_ac_current",
    "kombi_remaining_electric_range": "estimated_range",
    "mileage": "odometer", # Yes, mileage is in km no need to convert it
    "soc_hv_header": "soc",
    "capacity": "capacity_according_to_data_provider",
    "model": "model_according_to_data_provider",
    # Tesla
    "battery_level": "soc",
    "readable_date": "date",
    "charging_state": "charging_status",
    "fast_charger_type": "charging_method",
    "charge_rate": "charging_rate",
    "charger_power": "charging_power",
}

# The keys will be used to determine what columns to keep.
COL_DTYPES = {
    # Common
    "vin": CategoricalDtype(),
    "soc": "float32",
    "odometer": "float32",
    "estimated_range": "float32",
    "outside_temp": "float32",
    "unit": CategoricalDtype(),
    "date": "datetime64[ns]",
    "battery_energy": "float32",
    "charging_plug_connected": "bool", #BMW and Mobilisight
    "charging_status": CategoricalDtype(), #BMW, Tesla and Mobilisight
    "minutes_to_full_charge": "float32", #Tesla and Mobilisight
    "charging_method": CategoricalDtype(), #BMW and Mobilisight
    # Mercedes
    "max_range": "float32",
    # BMW
    "charging_ac_current": "float32",
    "charging_ac_voltage": "float32",
    "coolant_temperature": "float32",
    "kombi_remaining_electric_range": "float32",
    # Tesla
    "battery_heater": "bool",
    "battery_heater_no_power": "bool",
    "fast_charger_present": "bool",
    "power": "float32",
    "speed": "float32",
    "battery_level": "float32",
    "battery_range": "float32",
    "charge_current_request": "float32",
    "charge_current_request_max": "float32",
    "charge_enable_request": "float32",
    "charge_energy_added": "float32",
    "charge_limit_soc": "float32",
    "charge_limit_soc_max": "float32",
    "charge_limit_soc_min": "float32",
    "charge_limit_soc_std": "float32",
    "charge_miles_added_ideal": "float32",
    "charge_miles_added_rated": "float32",
    "charge_rate": "float32",
    "charger_actual_current": "float32",
    "charger_pilot_current": "float32",
    "charging_power": "float32",
    "charger_voltage": "float32",
    "est_battery_range": "float32",
    "inside_temp": "float32",
    "fast_charger_type": CategoricalDtype(),
    # Mobilisight
    "oil_temp": "float32",
    "coolant_temp": "float32",
    "charging_planned": "float32",
    "charging_rate": "float32",
    "engine_speed": "float32",
    "soh_oem": "float32",
}

DISCHARGE_VARS_TO_MEASURE = ["soc", "odometer", "estimated_range"]
COLS_TO_FILL = [
    "charging_status",
    "soc",
    "odometer",
    "estimated_range",
]

MAX_TIME_DIFF_TO_FILL = pd.Timedelta(minutes=45)

IN_CHARGE_CHARGING_STATUS_VALS = [
    'charging', # Tesla
    # 'nopower', # Tesla
    'chargingactive',
    'slow_charging',
    'fast_charging',
    'initialization',
    "in-progress",
]

IN_DISCHARGE_CHARGING_STATUS_VALS = [
    'charging_error',
    'nocharging',
    'chargingerror',
    'cable_unplugged',
    'disconnected', # Tesla
]

CHARGING_STATUS_VAL_TO_MASK = {
    #BMW
    "CHARGINGACTIVE": True,
    "slow_charging": True,
    "fast_charging": True,
    "charging_complete": False,
    "Charging": True,

    "charging_error": False,
    # Tesla
    "<NA>": False,
    pd.NA: False,
    "NOCHARGING": False,
    "CHARGINGENDED": False,
    "CHARGINGERROR": False,
    "INITIALIZATION": False,
    "CHARGINGPAUSED": False,
    # Mobilisight
    "cable_unplugged": False,
    "Disconnected": False,
}

CHARGE_MASK_WITH_CHARGING_STATUS_MAKES = [
    "bmw",
    "mercedes-benz",
    "ford",
    "volvo-cars",
    "stellantis",
    "tesla",
]

CHARGE_MASK_WITH_SOC_DIFFS_MAKES = [
    "kia",
    "renault",
]
MAX_TD = TD(hours=1, minutes=30)

NO_CHARGING_STATUS_COL_ERROR = "charging_status column not found in tss while trying to compute charging and discharging masks."

MAKE_NOT_SUPPORTED_ERROR = """
It is unclear how to compute charging and discharging masks for {make}.
Please add it to the CHARGE_MASK_WITH_CHARGING_STATUS_MAKES or CHARGE_MASK_WITH_SOC_DIFFS_MAKES lists.
"""

ALL_MAKES = CHARGE_MASK_WITH_CHARGING_STATUS_MAKES + CHARGE_MASK_WITH_SOC_DIFFS_MAKES

COLS_TO_STR_LOWER = [
    "unit",
    "charging_status",
    "charging_method",
    "fast_charger_type",
]
