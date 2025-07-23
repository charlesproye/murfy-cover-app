from pandas.api.types import CategoricalDtype
from pyspark.sql.types import FloatType, TimestampType, StringType, BooleanType
from pandas import Timedelta as TD
import pandas as pd

S3_PROCESSED_TSS_KEY_FORMAT = 'processed_ts/{make}/time_series/dev_processed_ts_spark.parquet'

NB_CORES_CLUSTER = 8


# Coefficient to scale soc to 0-100%
SCALE_SOC = {
    'tesla-fleet-telemetry': 1,
    'mercedes-benz': 100,
    'volvo-cars': 100,
    'kia': 100,
    'renault': 100,
    'ford': 100,
    'stellantis': 100,
    'bmw': 1
}


# Minimum threshold to consider a charging or discharging phase
SOC_DIFF_THRESHOLD = {
    'tesla-fleet-telemetry': 0.05,
    'mercedes-benz': 0.05,
    'volvo-cars': 0.05,
    'kia': 0.05,
    'renault': 0.05,
    'ford': 0.05,
    'stellantis': 0.05,
    'bmw': 0.05
}

# Columns to keep in order to run processed_ts + not used but necessary afterwards
NECESSARY_COLS = {
    'tesla': ['vin', 'date', 'odometer', 'soc', 'charging_status', 'dc_charge_energy_added', 'ac_charge_energy_added'],
    'renault': ['vin', 'date', 'odometer', 'soc'],
    'kia': ['vin', 'date', 'odometer', 'soc'],
    'bmw': ['vin', 'date', 'odometer', 'soc', 'charging_status'],
    'mercedes-benz': ['vin', 'date', 'odometer', 'soc', 'charging_status', 'total_charging_duration', 'start_time', 'end_time', 'energy_charged', 'displayed_state_of_charge', 'displayed_start_state_of_charge', 'charging_rate', 'estimated_range', 'fully_charged_end_times'],
    'ford': ['vin', 'date', 'odometer', 'soc', 'charging_status'],
    'volvo-cars': ['vin', 'date', 'odometer', 'soc', 'charging_status'],
    'stellantis': ['vin', 'date', 'odometer', 'soc', 'charging_status'],
    'tesla-fleet-telemetry': ['vin', 'date', 'odometer', 'soc', 'charging_status', 'dc_charge_energy_added', 'ac_charge_energy_added']
}

RENAME_COLS_DICT:dict[str, str] = {
    # High mobility
    "date_of_value": "date",
    "diagnostics_odometer": "odometer",
    "odometer_value": "odometer",
    "mileage_km": "odometer",
    "mileage": "odometer",
    "charging_battery_energy": "battery_energy",
    "charging_estimated_range": "estimated_range",
    "charging_battery_level": "soc",
    "battery_level": "soc",
    "soc_hv_header": "soc",
    "climate_outside_temperature": "outside_temp",
    "charging_status": "charging_status",
    "status": "charging_status",
    "charging_battery_charge_type": "charging_method",
    "usage_electric_consumption_average": 'consumption',
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
    "electricity.battery.stateOfHealth.percentage": "soh_oem",
    # Mercedes
    "charging_max_range": "max_range",
    "charging_charging_rate": "charging_rate",
    "charging_fully_charged_end_times": "fully_charged_end_times",
    # BMW
    "charging_ac_ampere": "charging_ac_current",
    "kombi_remaining_electric_range": "estimated_range",
    # "mileage": "odometer", # SUPPRIMÉ - doublon avec ligne 40
    # "soc_hv_header": "soc", # SUPPRIMÉ - doublon avec ligne 46
    "capacity": "capacity_according_to_data_provider",
    "model": "model_according_to_data_provider",
    "avg_electric_range_consumption": "consumption",
    # Tesla
    "readable_date": "date",
    "charging_state": "charging_status",
    "fast_charger_type": "charging_method",
    "charge_rate": "charging_rate",
    "charger_power": "charging_power",
    # fleet telemetry
    'BatteryLevel_stringValue': 'battery_level',
    'Soc_stringValue': 'soc',
    'OutsideTemp_stringValue': 'outside_temp',
    'Odometer_stringValue': 'odometer',
    'RatedRange_stringValue': 'rated_range',
    'ChargeCurrentRequest_stringValue': 'charge_current_request',
    'PackCurrent_stringValue': 'pack_current',
    'ChargeRateMilePerHour_doubleValue': 'charge_rate_mileper_hour',
    'CarType_stringValue': 'model',
    'EnergyRemaining_stringValue': 'energy_remaining',
    'DetailedChargeState_detailedChargeStateValue': 'charging_status',
    'InsideTemp_stringValue': 'inside_temp',
    'DCChargingPower_stringValue': 'dc_charging_power',
    'VehicleSpeed_stringValue': 'speed',
    'ChargeLimitSoc_stringValue': 'charge_limit_soc',
    'IdealBatteryRange_stringValue': 'battery_range',
    'RearDefrostEnabled_booleanValue': 'rear_defrost_enabled',
    'ChargePort_stringValue': 'charge_port',
    'BmsFullchargecomplete_stringValue': 'bms_full_charge_complete',
    'ACChargingPower_stringValue': 'ac_charging_power',
    'ACChargingEnergyIn_stringValue': 'ac_charge_energy_added', 
    'FastChargerPresent_stringValue': 'fast_charger_present',
    'DCChargingEnergyIn_stringValue': 'dc_charge_energy_added',
    # Spark  
    "Odometer" : "odometer",
    "ACChargingEnergyIn": "ac_charge_energy_added",
    "Soc": "soc",
    "CarType": 'model',
    "DCChargingEnergyIn": "dc_charge_energy_added",
    "BatteryLevel": "battery_level",
    "ACChargingPower": "ac_charging_power",
    "DCChargingPower": "dc_charging_power",
    "DetailedChargeState": "charging_status",
}




