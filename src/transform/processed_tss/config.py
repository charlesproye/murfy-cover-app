from pandas.api.types import CategoricalDtype
from pandas import Timedelta as TD
import pandas as pd

S3_PROCESSED_TSS_KEY_FORMAT = 'processed_ts/{make}/time_series/processed_tss_spark.parquet'

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
    #"battery_level": "soc",
    "readable_date": "date",
    "charging_state": "charging_status",
    "fast_charger_type": "charging_method",
    "charge_rate": "charging_rate",
    "charger_power": "charging_power",
    # fleet telemetry
    'readable_date': 'date',
    'BatteryLevel_stringValue': 'battery_level',
    'Soc_stringValue': 'soc',
    'OutsideTemp_stringValue': 'outside_temp',
    #'LifetimeEnergyUsed_stringValue': 'lifetime_energy_used',
    #'DefrostMode_defrostModeValue': 'defrost_mode',
    #'BrickVoltageMin_stringValue': 'brick_voltage_min',
    'Odometer_stringValue': 'odometer',
    'RatedRange_stringValue': 'rated_range', # True battery range lower than battery range line 98
    #'HvacAutoMode_hvacAutoModeValue': 'hvac_auto_mode',
    'ChargeCurrentRequest_stringValue': 'charge_current_request',
    'PackCurrent_stringValue': 'pack_current',
    #'HvacACEnabled_booleanValue': 'hvac_ace_nabled',
    'ChargeRateMilePerHour_doubleValue': 'charge_rate_mileper_hour',
    'CarType_stringValue': 'model',
    'EnergyRemaining_stringValue': 'energy_remaining', # energie en kWh
    'DetailedChargeState_detailedChargeStateValue': 'charging_status', # Etat de charge détiallé mieux que charge_state
    #'HvacPower_hvacPowerValue': 'hvac_power',
    'InsideTemp_stringValue': 'inside_temp',
    #'EstBatteryRange_stringValue': 'est_battery_range',
    #'ChargeCurrentRequestMax_stringValue': 'charge_current_request_max',
    'DCChargingPower_stringValue': 'dc_charging_power',
    #'PackVoltage_stringValue': 'pack_voltage',
    'VehicleSpeed_stringValue': 'speed',
    'ChargeLimitSoc_stringValue': 'charge_limit_soc',
    #'BMSState_stringValue': 'bms_state',
    #'DCDCEnable_stringValue': 'dc_dc_enable',
    'IdealBatteryRange_stringValue': 'battery_range', # Battery range classic
    'RearDefrostEnabled_booleanValue': 'rear_defrost_enabled',
    'ChargePort_stringValue': 'charge_port',
    'BmsFullchargecomplete_stringValue': 'bms_full_charge_complete',
    'ACChargingPower_stringValue': 'ac_charging_power',
    'ACChargingEnergyIn_stringValue': 'ac_charge_energy_added', 
    'FastChargerPresent_stringValue': 'fast_charger_present',
    # 'ModuleTempMin_stringValue': 'module_temp_min',
    'DCChargingEnergyIn_stringValue': 'dc_charge_energy_added',
    # 'ChargePortColdWeatherMode_stringValue': 'charge_port_cold_weather_mode',
    # 'ChargeAmps_stringValue': 'charge_amps',
    #'ChargeState_stringValue': 'charge_state',
    # 'ModuleTempMax_stringValue': 'module_temp_max',
    # 'EfficiencyPackage_stringValue': 'efficiency_package',
    # 'ChargeEnableRequest_stringValue': 'charge_enable_request',
    # 'BrickVoltageMax_stringValue': 'brick_voltage_max',
    # 'PreconditioningEnabled_stringValue': 'precondition_ingenabled',
    # 'DefrostForPreconditioning_booleanValue': 'defrost_for_preconditioning',
    # 'ChargerVoltage_doubleValue': 'charger_voltage',
    # 'ChargingCableType_cableTypeValue': 'charging_cable_type',
    # 'EstimatedHoursToChargeTermination_doubleValue': 'estimated_hours_to_charge_termination',
    # 'FastChargerType_fastChargerValue': 'fast_charger_type',
    # 'ChargerPhases_stringValue': 'charger_phases',
    # 'BatteryHeaterOn_stringValue': 'battery_heater'
    
    ########## Spark  
    "readable_date": "date",
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
    # "tesla",
    # "bmw",
    # "mercedes-benz",
    # "ford",
    # "volvo-cars",
    # "stellantis",
    "tesla-fleet-telemetry"
]

CHARGE_MASK_WITH_SOC_DIFFS_MAKES = [
    # "kia",
    # "renault",
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

