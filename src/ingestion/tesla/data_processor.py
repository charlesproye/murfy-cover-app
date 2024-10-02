import datetime
import logging

def extract_relevant_data(data, vehicle_id):
    try:
        relevant_data = {
            "vin": vehicle_id,
            "timestamp": data["response"]["charge_state"]["timestamp"],
            "readable_date": datetime.datetime.fromtimestamp(data["response"]["charge_state"]["timestamp"] / 1000.0).strftime('%Y-%m-%d %H:%M:%S'),
            "power": data["response"]["drive_state"]["power"],
            "speed": data["response"]["drive_state"]["speed"],
            "battery_heater": data["response"]["climate_state"]["battery_heater"],
            "battery_heater_no_power": data["response"]["climate_state"]["battery_heater_no_power"],
            "minutes_to_full_charge": data["response"]["charge_state"]["minutes_to_full_charge"],
            "battery_level": data["response"]["charge_state"]["battery_level"],
            "battery_range": data["response"]["charge_state"]["battery_range"],
            "charge_current_request": data["response"]["charge_state"]["charge_current_request"],
            "charge_current_request_max": data["response"]["charge_state"]["charge_current_request_max"],
            "charge_enable_request": data["response"]["charge_state"]["charge_enable_request"],
            "charge_energy_added": data["response"]["charge_state"]["charge_energy_added"],
            "charge_limit_soc": data["response"]["charge_state"]["charge_limit_soc"],
            "charge_limit_soc_max": data["response"]["charge_state"]["charge_limit_soc_max"],
            "charge_limit_soc_min": data["response"]["charge_state"]["charge_limit_soc_min"],
            "charge_limit_soc_std": data["response"]["charge_state"]["charge_limit_soc_std"],
            "charge_miles_added_ideal": data["response"]["charge_state"]["charge_miles_added_ideal"],
            "charge_miles_added_rated": data["response"]["charge_state"]["charge_miles_added_rated"],
            "charge_port_cold_weather_mode": data["response"]["charge_state"]["charge_port_cold_weather_mode"],
            "charge_rate": data["response"]["charge_state"]["charge_rate"],
            "charger_actual_current": data["response"]["charge_state"]["charger_actual_current"],
            "charger_pilot_current": data["response"]["charge_state"]["charger_pilot_current"],
            "charger_power": data["response"]["charge_state"]["charger_power"],
            "charger_voltage": data["response"]["charge_state"]["charger_voltage"],
            "charging_state": data["response"]["charge_state"]["charging_state"],
            "est_battery_range": data["response"]["charge_state"]["est_battery_range"],
            "fast_charger_present": data["response"]["charge_state"]["fast_charger_present"],
            "fast_charger_type": data["response"]["charge_state"]["fast_charger_type"],
            "odometer": data["response"]["vehicle_state"]["odometer"],
            "inside_temp": data["response"]["climate_state"]["inside_temp"],
            "outside_temp": data["response"]["climate_state"]["outside_temp"],
            "model": data["response"]["vehicle_config"]["car_type"]
        }
        return relevant_data
    except KeyError as e:
        logging.error(f"KeyError extracting data: {e}")
        return None
