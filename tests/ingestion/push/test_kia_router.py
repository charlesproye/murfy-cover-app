"""Minimal tests for Kia-specific functionality."""

import os

# Set required env vars before importing
os.environ.setdefault("JWT_SECRET", "test-secret-key")
os.environ.setdefault("KIA_PUSH_API_KEY", "test-key")

from ingestion.push.kia_router import _extract_kia_timestamps, _remove_meta_fields


def test_extract_kia_timestamps():
    """Test that meta.* fields are converted to timestamps for state.* fields."""
    record_data = {
        "meta": {
            "Vehicle": {
                "Drivetrain": {"Odometer": 1704196800000},  # 2024-01-02 12:00:00 UTC
                "Green": {
                    "BatteryManagement": {"SoC": 1704200400000}
                },  # 2024-01-02 13:00:00 UTC
            }
        },
        "state": {
            "Vehicle": {
                "Drivetrain": {"Odometer": 12345},
                "Green": {"BatteryManagement": {"SoC": 85.5}},
            }
        },
    }

    field_timestamps = _extract_kia_timestamps(record_data)

    # Check that state fields got timestamps from meta fields
    assert "state.Vehicle.Drivetrain.Odometer" in field_timestamps
    assert "state.Vehicle.Green.BatteryManagement.SoC" in field_timestamps

    # Verify timestamp conversion (Unix ms to ISO)
    odometer_ts = field_timestamps["state.Vehicle.Drivetrain.Odometer"]
    assert odometer_ts.startswith("2024-01-02T12:00:00")

    soc_ts = field_timestamps["state.Vehicle.Green.BatteryManagement.SoC"]
    assert soc_ts.startswith("2024-01-02T13:00:00")


def test_remove_meta_fields():
    """Test that meta fields are removed from data."""
    data = {
        "meta": {"Vehicle": {"Drivetrain": {"Odometer": 1704196800000}}},
        "state": {"Vehicle": {"Drivetrain": {"Odometer": 12345}}},
        "header": {"vin": "VIN123"},
        "received_date": "2024-01-02T12:00:00Z",
    }

    cleaned = _remove_meta_fields(data)

    # meta should be removed
    assert "meta" not in cleaned

    # other fields should remain
    assert "state" in cleaned
    assert "header" in cleaned
    assert "received_date" in cleaned
    assert cleaned["state"]["Vehicle"]["Drivetrain"]["Odometer"] == 12345


def test_remove_meta_fields_nested():
    """Test that nested meta removal works correctly."""
    data = {
        "meta": {"some": "data"},
        "nested": {"level1": {"level2": {"value": 123}}},
    }

    cleaned = _remove_meta_fields(data)

    assert "meta" not in cleaned
    assert cleaned["nested"]["level1"]["level2"]["value"] == 123
