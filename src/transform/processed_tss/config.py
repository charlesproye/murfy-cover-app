from pandas.api.types import CategoricalDtype
from pyspark.sql.types import FloatType, TimestampType, StringType, BooleanType
from pandas import Timedelta as TD
import pandas as pd

S3_PROCESSED_TSS_KEY_FORMAT = 'processed_ts/{make}/time_series/dev_processed_ts_spark.parquet'

NB_CORES_CLUSTER = 8

ODOMETER_MILES_TO_KM = {
    "tesla": 1.60934,
    "tesla-fleet-telemetry": 1.60934,
}

COLS_TO_CPY_FROM_FLEET_INFO = [
    "make",
    "model",
    "version",
    "capacity",
    "owner",
    "range",
]

SCALE_SOC = {
    'tesla-fleet-telemetry': 100,
    'mercedes-benz': 1,
    'volvo-cars': 1,
    'kia': 1,
    'renault': 1,
    'ford': 1,
    'stellantis': 1,
    'bmw': 100
}

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

COL_TO_SELECT = [
    'vin', 'date', 'odometer', 'soc', 
    "battery_level",
    "ac_charge_energy_added",
    "dc_charge_energy_added",
    "ac_charging_power",
    "dc_charging_power",
    "charging_status"]

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
    "consumption": "float32",
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
    # Fleet-telemetry
    "charging_state": "category",
    "ac_charge_energy_added":'float32',
    "dc_charge_energy_added": 'float32',
    "ac_charging_power": 'float32',
    "dc_charging_power": 'float32',
}


COL_DTYPES_SPARK = {
    
        "date": TimestampType(), 
        "soc": FloatType(), 
        "odometer": FloatType(), 
        "battery_level": FloatType(), 
        "ac_charge_energy_added": FloatType(), 
        "dc_charge_energy_added": FloatType(), 
        "ac_charging_power": FloatType(), 
        "dc_charging_power": FloatType(), 
        "charging_status": StringType(), 
        "prev_date": TimestampType(), 
        "sec_time_diff": FloatType(), 
        "in_charge": BooleanType(), 
        "in_discharge": BooleanType(), 
        "charge_energy_added": FloatType(), 
        "soc_diff": FloatType(), 
        "in_charge_idx": FloatType(), 
        "end_of_contract_date": TimestampType(), 
        "fleet_name": StringType(), 
        "start_date": TimestampType(), 
        "model": StringType(), 
        "version": StringType(), 
        "capacity": FloatType(), 
        "net_capacity": FloatType(), 
        "range": FloatType(), 
        "tesla_code": StringType(), 
        "make": StringType(),
        "region_name": StringType(),
        "activation_status": BooleanType(),
        "vin": StringType()
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
    # fleet-telemetry
    'detailedchargestatecharging', 
    'detailedchargestatestarting'
]

IN_DISCHARGE_CHARGING_STATUS_VALS = [
    'charging_error',
    'nocharging',
    'chargingerror',
    'cable_unplugged',
    'disconnected', # Tesla
     # fleet-telemetry
    "detailedchargestatedisconnected",
    "detailedchargestatenopower",
    "detailedchargestatestopped",
    "detailedchargestatecomplete",
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
    #fleet-telemetry
    "DetailedChargeStateDisconnected": False
}

CHARGE_MASK_WITH_CHARGING_STATUS_MAKES = [
    "tesla",
    "bmw",
    "mercedes-benz",
    "ford",
    "volvo-cars",
    "stellantis",
    "tesla-fleet-telemetry"
]

CHARGE_MASK_WITH_SOC_DIFFS_MAKES = [
    "kia",
    "renault"
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

# Tesla specific vars:
MIN_POWER_LOSS = -0.0005
MAX_CHARGE_TD = TD(days=1)



