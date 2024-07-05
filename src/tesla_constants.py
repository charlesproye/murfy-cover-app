import numpy as np
from datetime import datetime, timedelta
from datetime import timedelta as TD
import os
import pandas as pd

# data types
DATA_TYPE_RAW_DF_DICT = {
    # "date": ,
    "vin": "string",
    # "timestamp": ,
    "power": float,
    "speed": float,
    # "battery_heater": pd.CategoricalDtype(),
    "battery_heater_no_power": bool,
    "minutes_to_full_charge": int,
    "battery_level": int,
    "battery_range": float,
    "charge_current_request": float,
    "charge_current_request_max": float,
    "charge_enable_request": float,
    "charge_energy_added": float,
    "charge_limit_soc": float,
    "charge_limit_soc_max": float,
    "charge_limit_soc_min": float,
    "charge_limit_soc_std": float,
    "charge_miles_added_ideal": float,
    "charge_miles_added_rated": float,
    # "charge_port_cold_weather_mode": pd.CategoricalDtype(),
    "charge_rate": float,
    "charger_actual_current": float,
    "charger_pilot_current": float,
    "charger_power": float,
    "charger_voltage": float,
    # "charging_state": pd.CategoricalDtype(),
    "est_battery_range": float,
    "fast_charger_present": bool,
    # "fast_charger_type": pd.CategoricalDtype(),
    "odometer": float,
    "inside_temp": float,
    "outside_temp": float,
}
DATA_TYPE_RAW_DF_DICT_FOR_EXTRAS = {k: v for k, v in DATA_TYPE_RAW_DF_DICT.items() if k != "vin"}
# path
PATH_TO_MODELS_INFO = "data_cache/models_info.csv"
PATH_TO_FLEET_INFO_FOLDER = "data_cache/tesla/fleet_info"
PATH_TO_TESLA_PROFILE = "data_cache/tesla/profile/{vin}.parquet"
PATH_TO_RAW_TESLA_TS_FOLER = "data_cache/tesla/raw_time_series/"
PATH_TO_RAW_TESLA_TS = "data_cache/tesla/raw_time_series/{vin}.parquet"
PATH_TO_PROCESSED_TESLA_TS = "data_cache/tesla/processed_time_series/{vin}.parquet"
PATH_TO_VEHICLES_INFO_DF = os.path.join("data_cache", "vehicles_info", "vehicles_info.parquet")
NOW = datetime.now()
RAW_VEHICLE_DF_DATE_MARGIN = timedelta(days=1)
EWM_SPAN_ENERGY_TO_RANGE_RATIO = 10

# Convertions
MILE_TO_KM = 1.60934
# Tesla
#model y rear drive
MODEL_Y_REAR_DRIVE_MIN_RANGE = 295 * MILE_TO_KM
MODEL_Y_REAR_DRIVE_MIN_KM_PER_SOC = MODEL_Y_REAR_DRIVE_MIN_RANGE / 100
MODEL_Y_REAR_DRIVE_STOCK_KJ = 291600
MODEL_Y_REAR_DRIVE_STOCK_KWH_PER_SOC = 0.81

