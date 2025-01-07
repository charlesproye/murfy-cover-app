import datetime
import logging

def extract_relevant_data(data, vehicle_id):
    try:
        if not isinstance(data, dict) or 'response' not in data:
            logging.error(f"Invalid data format for vehicle {vehicle_id}. Data type: {type(data)}")
            return None

        response = data.get('response', {})
        if not isinstance(response, dict):
            logging.error(f"Invalid response format for vehicle {vehicle_id}. Response type: {type(response)}")
            return None

        relevant_data = {
            "vin": vehicle_id,
            "timestamp": response.get("charge_state", {}).get("timestamp"),
            "readable_date": datetime.datetime.fromtimestamp(
                response.get("charge_state", {}).get("timestamp", 0) / 1000.0
            ).strftime('%Y-%m-%d %H:%M:%S'),
            "power": response.get("drive_state", {}).get("power"),
            "speed": response.get("drive_state", {}).get("speed"),
            "battery_heater": response.get("climate_state", {}).get("battery_heater"),
            "battery_heater_no_power": response.get("climate_state", {}).get("battery_heater_no_power"),
            "minutes_to_full_charge": response.get("charge_state", {}).get("minutes_to_full_charge"),
            "battery_level": response.get("charge_state", {}).get("battery_level"),
            "battery_range": response.get("charge_state", {}).get("battery_range"),
            "charge_current_request": response.get("charge_state", {}).get("charge_current_request"),
            "charge_current_request_max": response.get("charge_state", {}).get("charge_current_request_max"),
            "charge_enable_request": response.get("charge_state", {}).get("charge_enable_request"),
            "charge_energy_added": response.get("charge_state", {}).get("charge_energy_added"),
            "charge_limit_soc": response.get("charge_state", {}).get("charge_limit_soc"),
            "charge_limit_soc_max": response.get("charge_state", {}).get("charge_limit_soc_max"),
            "charge_limit_soc_min": response.get("charge_state", {}).get("charge_limit_soc_min"),
            "charge_limit_soc_std": response.get("charge_state", {}).get("charge_limit_soc_std"),
            "charge_miles_added_ideal": response.get("charge_state", {}).get("charge_miles_added_ideal"),
            "charge_miles_added_rated": response.get("charge_state", {}).get("charge_miles_added_rated"),
            "charge_port_cold_weather_mode": response.get("charge_state", {}).get("charge_port_cold_weather_mode"),
            "charge_rate": response.get("charge_state", {}).get("charge_rate"),
            "charger_actual_current": response.get("charge_state", {}).get("charger_actual_current"),
            "charger_pilot_current": response.get("charge_state", {}).get("charger_pilot_current"),
            "charger_power": response.get("charge_state", {}).get("charger_power"),
            "charger_voltage": response.get("charge_state", {}).get("charger_voltage"),
            "charging_state": response.get("charge_state", {}).get("charging_state"),
            "est_battery_range": response.get("charge_state", {}).get("est_battery_range"),
            "fast_charger_present": response.get("charge_state", {}).get("fast_charger_present"),
            "fast_charger_type": response.get("charge_state", {}).get("fast_charger_type"),
            "odometer": response.get("vehicle_state", {}).get("odometer"),
            "inside_temp": response.get("climate_state", {}).get("inside_temp"),
            "outside_temp": response.get("climate_state", {}).get("outside_temp"),
            "model": response.get("vehicle_config", {}).get("car_type")
        }
        return relevant_data
    except Exception as e:
        logging.error(f"Error extracting data for vehicle {vehicle_id}: {str(e)}")
        if isinstance(data, dict):
            logging.error(f"Data keys available: {data.keys()}")
        return None
