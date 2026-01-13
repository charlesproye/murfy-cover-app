"""Tests for Mobilisights utility functions."""

import pytest

from ingestion.conversion_utils import (
    convert_unit,
)


def test_convert_miles_to_km():
    """Test that miles are converted to kilometers."""
    json_data = {
        "odometer": {
            "value": 100.0,
            "unit": "mi",
        }
    }

    convert_unit(json_data, "odometer", "km")

    assert json_data["odometer"]["value"] == 160.9
    assert json_data["odometer"]["unit"] == "km"


def test_convert_speeds():
    """Test that speed from miles per hour is converted to kilometers per hour."""
    json_data = {
        "speed": {
            "value": 100.0,
            "unit": "mi/h",
        }
    }

    convert_unit(json_data, "speed", "km/h")

    assert json_data["speed"]["value"] == 160.9
    assert json_data["speed"]["unit"] == "km/h"


def test_convert_time():
    json_data = {
        "time": {
            "value": 100.0,
            "unit": "min",
        }
    }

    convert_unit(json_data, "time", "seconds")

    assert json_data["time"]["value"] == 6000
    assert json_data["time"]["unit"] == "seconds"


def test_convert_fahrenheit_to_celsius():
    """Test that Fahrenheit is converted to Celsius."""
    json_data = {
        "engine": {
            "coolant": {
                "temperature": {
                    "value": 212.0,  # 212°F = 100°C
                    "unit": "°F",
                }
            }
        }
    }

    convert_unit(json_data, "engine.coolant.temperature", "°C")

    assert json_data["engine"]["coolant"]["temperature"]["value"] == 100.0
    assert json_data["engine"]["coolant"]["temperature"]["unit"] == "°C"


def test_convert_unit_if_needed_invalid_unit():
    """Test that an invalid unit raises an error."""
    json_data = {
        "odometer": {
            "value": 100.0,
            "unit": "ft",
        }
    }
    with pytest.raises(ValueError):
        convert_unit(json_data, "odometer", "km")
