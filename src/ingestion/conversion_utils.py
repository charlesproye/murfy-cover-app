from typing import Any


def _convert_miles_to_km(value: float) -> float:
    return round(value * 1.609344, 1)


def _convert_fahrenheit_to_celsius(value: float) -> float:
    return round((value - 32) / 1.8, 1)


def _convert_minutes_to_seconds(value: float) -> float:
    return round(value * 60, 1)


def get_nested_value(data: dict, path: str) -> Any:
    """
    Get a nested value from a dictionary using dot-notation path.

    Args:
        data: The nested dictionary
        path: Dot-notation path (e.g., "odometer.value", "electricity.residualAutonomy.value")

    Returns:
        The value at the path, or None if the path doesn't exist
    """
    keys = path.split(".")
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _set_nested_value(data: dict, path: str, value: Any) -> None:
    """
    Set a nested value in a dictionary using dot-notation path.
    Creates intermediate dictionaries if they don't exist.

    Args:
        data: The nested dictionary to modify in-place
        path: Dot-notation path (e.g., "odometer.value", "electricity.residualAutonomy.value")
        value: The value to set
    """
    keys = path.split(".")
    current = data
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            # Path conflict - existing value is not a dict
            return
        current = current[key]
    current[keys[-1]] = value


def convert_unit(
    json_data: dict,
    field_prefix: str,
    new_unit: str,
) -> None:
    """
    Convert a unit in json_data if it matches the expected unit.

    Args:
        json_data: The data dictionary to modify in-place (nested structure)
        field_prefix: The field prefix (e.g., "odometer", "speed", "electricity.residualAutonomy")
        expected_unit: The unit to match against (e.g., "mi", "°F")
        new_unit: The unit to set after conversion (e.g., "km", "°C")
    """
    value_path = f"{field_prefix}.value"
    unit_path = f"{field_prefix}.unit"

    value = get_nested_value(json_data, value_path)
    if value is None:
        return

    current_unit = (get_nested_value(json_data, unit_path) or "").lower()

    if current_unit == "":
        raise ValueError(
            f"Unit is required for {field_prefix}. Got unit: {current_unit}"
        )

    miles_units = {"mi", "mi/h", "miles"}
    kilometers_units = {"km", "km/h", "kilometers"}
    minutes_units = {"min", "minutes"}
    seconds_units = {"sec", "seconds"}

    new_unit_lower = new_unit.lower()
    if current_unit.lower() == new_unit_lower:
        return
    elif current_unit.lower() in miles_units and new_unit_lower in kilometers_units:
        converter_func = _convert_miles_to_km
    elif current_unit == "°f" and new_unit_lower == "°c":
        converter_func = _convert_fahrenheit_to_celsius
    elif current_unit.lower() in minutes_units and new_unit_lower in seconds_units:
        converter_func = _convert_minutes_to_seconds
    else:
        raise ValueError(
            f"Unsupported unit: {current_unit} for {field_prefix}. Something that can be converted to {new_unit}"
        )

    _set_nested_value(json_data, value_path, converter_func(value))
    _set_nested_value(json_data, unit_path, new_unit)
