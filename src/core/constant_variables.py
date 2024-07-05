"""
Yes, it is weird to have a module called constant_variables buy we live in a weird world.
"""
KJ_TO_KWH = 0.00027777777
DEFAULT_DIFF_VARS = {
    "soc": "soc_diff",
    "date": "duration",
}

YES =  [
    'battery_heater',
    'battery_heater_no_power',
    'ideal_battery_range',
    # 'max_range_charge_counter',
    'battery_heater_on',
    'battery_level',
    # 'battery_range',
    'charge_amps',
    'charge_current_request',
    'charge_current_request_max',
    'charge_limit_soc',
    'charge_limit_soc_max',
    'charge_limit_soc_min',
    'charge_limit_soc_std',
    'charge_miles_added_ideal',
    'charge_miles_added_rated',
    'charge_port_cold_weather_mode',
    'charge_rate',
    'charger_actual_current',
    'charger_phases',
    'charger_pilot_current',
    'charger_power',
    'charger_voltage',
    'charging_state',
    # 'est_battery_range',
    'fast_charger_brand',
    'fast_charger_present',
    'fast_charger_type', 
    'inside_temp',
    'outside_temp'
]
