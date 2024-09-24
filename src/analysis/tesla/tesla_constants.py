from os.path import dirname, join

from core.constants import *

# path
JSON_FLEET_INFO_RESPONSE_PATH = join(dirname(__file__), "data_cache/api_responses/raw_fleet_info.json")
INITIAL_FLEET_INFO_PATH = join(dirname(__file__), "data_cache/initial_fleet_info.parquet")
TS_RESPONSES_REGEX_PATH = join(dirname(__file__), "data_cache/api_responses/*.csv")
RAW_TSS_PATH = join(dirname(__file__), "data_cache/raw_tss.parquet")

# data types
DATA_TYPE_RAW_DF_DICT = {
    # "date": ,
    "vin": "string",
    # "timestamp": ,
    "power": "float",
    "speed": "float",
    # "battery_heater": pd.CategoricalDtype(),
    "battery_heater_no_power": bool,
    "minutes_to_full_charge": int,
    "battery_level": int,
    "battery_range": "float",
    "charge_current_request": "float",
    "charge_current_request_max": "float",
    "charge_enable_request": "float",
    "charge_energy_added": "float",
    "charge_limit_soc": "float",
    "charge_limit_soc_max": "float",
    "charge_limit_soc_min": "float",
    "charge_limit_soc_std": "float",
    "charge_miles_added_ideal": "float",
    "charge_miles_added_rated": "float",
    # "charge_port_cold_weather_mode": pd.CategoricalDtype(),
    "charge_rate": "float",
    "charger_actual_current": "float",
    "charger_pilot_current": "float",
    "charger_power": "float",
    "charger_voltage": "float",
    # "charging_state": pd.CategoricalDtype(),
    "est_battery_range": "float",
    "fast_charger_present": bool,
    # "fast_charger_type": pd.CategoricalDtype(),
    "odometer": "float",
    "inside_temp": "float",
    "outside_temp": "float",
}
DATA_TYPE_RAW_DF_DICT_FOR_EXTRAS = {k: v for k, v in DATA_TYPE_RAW_DF_DICT.items() if k != "vin"}
# Tesla
#model y rear drive
MODEL_Y_REAR_DRIVE_MIN_RANGE = 295 * MILES_TO_KM
MODEL_Y_REAR_DRIVE_MIN_KM_PER_SOC = MODEL_Y_REAR_DRIVE_MIN_RANGE / 100
MODEL_Y_REAR_DRIVE_STOCK_KJ = 291600
MODEL_Y_REAR_DRIVE_STOCK_KWH_PER_SOC = 0.81

