from ingestion.conversion_utils import convert_unit, get_nested_value


def convert_json_units(json_data: dict) -> dict:
    # Convert distances from miles to kilometers
    convert_unit(json_data, "odometer", "km")
    convert_unit(json_data, "speed", "km/h")
    convert_unit(json_data, "electricity.residualAutonomy", "km")
    convert_unit(json_data, "ecodriving.odometer", "km")
    convert_unit(json_data, "maintenance.odometer", "km")

    # Convert temperatures from Fahrenheit to Celsius
    convert_unit(json_data, "engine.coolant.temperature", "°C")
    convert_unit(json_data, "externaltemperature", "°C")

    # Validate voltage unit
    voltage_value = get_nested_value(json_data, "engine.battery.voltage.value")
    voltage_unit = get_nested_value(json_data, "engine.battery.voltage.unit") or ""
    if voltage_value and voltage_unit.upper() != "V":
        raise ValueError(
            f"engine.battery.voltage.unit is not supported: {voltage_unit}"
        )

    return json_data
