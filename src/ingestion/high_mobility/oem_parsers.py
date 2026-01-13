from ingestion.conversion_utils import convert_unit, get_nested_value


def process_renault(vin: str, json_data: dict) -> dict:
    """Process Renault-specific data transformations."""
    convert_unit(json_data, "diagnostics.odometer.data", "kilometers")
    convert_unit(json_data, "charging.estimated_range.data", "kilometers")

    # Boolean conversion for plugged_in
    plugged_in_data = get_nested_value(json_data, "charging.plugged_in.data")
    if plugged_in_data is not None and isinstance(plugged_in_data, str):
        json_data["charging"]["plugged_in"]["data"] = plugged_in_data == "plugged_in"

    return json_data


def process_mercedes_benz(vin: str, json_data: dict) -> dict:
    """Process Mercedes-Benz-specific data transformations."""
    convert_unit(json_data, "diagnostics.odometer.data", "kilometers")
    convert_unit(json_data, "charging.estimated_range.data", "kilometers")
    convert_unit(json_data, "usage.electric_distance_last_trip.data", "kilometers")
    convert_unit(json_data, "usage.electric_distance_since_reset.data", "kilometers")
    convert_unit(json_data, "diagnostics.engine_coolant_temperature.data", "°C")

    return json_data


def process_ford(vin: str, json_data: dict) -> dict:
    """Process Ford-specific data transformations."""
    convert_unit(json_data, "diagnostics.odometer.data", "kilometers")
    convert_unit(json_data, "charging.time_to_complete_charge.data", "seconds")

    return json_data


def process_volvo_cars(vin: str, json_data: dict) -> dict:
    """Process Volvo-Cars-specific data transformations."""
    convert_unit(json_data, "diagnostics.odometer.data", "kilometers")
    convert_unit(json_data, "charging.estimated_range.data", "kilometers")
    convert_unit(json_data, "diagnostics.distance_since_reset.data", "kilometers")
    convert_unit(json_data, "diagnostics.estimated_range.data", "kilometers")

    convert_unit(json_data, "charging.time_to_complete_charge.data", "seconds")

    # Boolean conversion for plugged_in
    plugged_in_data = get_nested_value(json_data, "charging.plugged_in.data")
    if plugged_in_data is not None and isinstance(plugged_in_data, str):
        json_data["charging"]["plugged_in"]["data"] = plugged_in_data == "plugged_in"

    return json_data
