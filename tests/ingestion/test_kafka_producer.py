"""Minimal tests for Kafka producer functionality."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.models import MakeEnum
from ingestion.kafka_producer import KafkaProducerService


@pytest.fixture
def kafka_producer():
    """Create a KafkaProducerService instance with mocked dependencies."""
    producer = KafkaProducerService()
    producer.producer = AsyncMock()
    producer.redis = AsyncMock()

    # Mock field filter to return test fields
    producer.field_filter.get_filter = MagicMock(  # type: ignore[assignment]
        return_value={
            "state.Vehicle.Drivetrain.Odometer": "int",
            "state.Vehicle.Green.BatteryManagement.SoC": "double",
        }
    )

    return producer


@pytest.mark.asyncio
async def test_field_filtering(kafka_producer):
    """Test that only fields from CSV are sent to Kafka."""
    data = {
        "state": {
            "Vehicle": {
                "Drivetrain": {"Odometer": 12345},
                "Green": {
                    "BatteryManagement": {
                        "SoC": 85.5,
                        "Temperature": 22.3,  # Not in CSV
                    }
                },
            }
        },
        "received_date": "2026-01-02T12:00:00Z",
    }

    # Mock Redis to always return "value changed" (async method)
    kafka_producer.redis.get = AsyncMock(return_value=None)

    messages_sent = await kafka_producer.send_filtered_data(
        MakeEnum.kia, data, "VIN123"
    )

    # Should send 2 messages (only fields in CSV)
    assert messages_sent == 2
    assert kafka_producer.producer.send.call_count == 2


@pytest.mark.asyncio
async def test_change_detection(kafka_producer):
    """Test that unchanged values are not sent to Kafka."""
    data = {
        "state": {"Vehicle": {"Drivetrain": {"Odometer": 12345}}},
        "received_date": "2026-01-02T12:00:00Z",
    }

    # First call - value is new
    kafka_producer.redis.get = AsyncMock(return_value=None)
    messages_sent = await kafka_producer.send_filtered_data(
        MakeEnum.kia, data, "VIN123"
    )
    assert messages_sent == 1

    # Second call - value unchanged (Redis returns same cached value)
    import msgspec

    cached_value = msgspec.json.encode(12345)
    kafka_producer.redis.get = AsyncMock(return_value=cached_value)

    messages_sent = await kafka_producer.send_filtered_data(
        MakeEnum.kia, data, "VIN123"
    )
    assert messages_sent == 0  # No messages sent (value unchanged)


@pytest.mark.asyncio
async def test_field_specific_timestamps(kafka_producer):
    """Test that field-specific timestamps are used when provided."""
    data = {
        "state": {"Vehicle": {"Drivetrain": {"Odometer": 12345}}},
        "received_date": "2026-01-02T12:00:00Z",
    }

    field_timestamps = {"state.Vehicle.Drivetrain.Odometer": "2026-01-02T14:30:00Z"}

    kafka_producer.redis.get = AsyncMock(return_value=None)

    await kafka_producer.send_filtered_data(
        MakeEnum.kia, data, "VIN123", field_timestamps=field_timestamps
    )

    # Check that the message was created with the field-specific timestamp
    call_args = kafka_producer.producer.send.call_args
    message = call_args.kwargs["value"]
    assert message["created_at"] == "2026-01-02T14:30:00"


@pytest.mark.asyncio
async def test_message_format(kafka_producer):
    """Test that Kafka messages have the correct format."""
    data = {
        "state": {"Vehicle": {"Drivetrain": {"Odometer": 12345}}},
        "received_date": "2026-01-02T12:00:00Z",
    }

    kafka_producer.redis.get = AsyncMock(return_value=None)

    await kafka_producer.send_filtered_data(MakeEnum.kia, data, "VIN123")

    call_args = kafka_producer.producer.send.call_args
    message = call_args.kwargs["value"]

    # Check message structure
    assert message["vin"] == "VIN123"
    assert message["metric_name"] == "state.Vehicle.Drivetrain.Odometer"
    assert message["value_int"] == 12345
    assert message["value_double"] is None
    assert message["value_string"] is None
    assert message["value_bool"] is None
    assert "created_at" in message


@pytest.mark.asyncio
async def test_type_conversion(kafka_producer):
    """Test that values are converted to correct type columns."""
    kafka_producer.field_filter.get_filter = MagicMock(
        return_value={
            "test.int_field": "int",
            "test.double_field": "double",
            "test.string_field": "string",
            "test.bool_field": "bool",
        }
    )

    data = {
        "test": {
            "int_field": 42,
            "double_field": 3.14,
            "string_field": "hello",
            "bool_field": True,
        }
    }

    kafka_producer.redis.get = AsyncMock(return_value=None)

    await kafka_producer.send_filtered_data(MakeEnum.kia, data, "VIN123")

    # Check each message was sent with correct type
    assert kafka_producer.producer.send.call_count == 4

    # Get all messages sent
    messages = [
        call.kwargs["value"] for call in kafka_producer.producer.send.call_args_list
    ]

    # Find each message by metric_name and verify type column
    int_msg = next(m for m in messages if m["metric_name"] == "test.int_field")
    assert int_msg["value_int"] == 42
    assert int_msg["value_double"] is None

    double_msg = next(m for m in messages if m["metric_name"] == "test.double_field")
    assert double_msg["value_double"] == 3.14
    assert double_msg["value_int"] is None

    string_msg = next(m for m in messages if m["metric_name"] == "test.string_field")
    assert string_msg["value_string"] == "hello"

    bool_msg = next(m for m in messages if m["metric_name"] == "test.bool_field")
    assert bool_msg["value_bool"] is True


@pytest.mark.asyncio
async def test_timezone_conversion(kafka_producer):
    """Test that non-UTC offsets are correctly converted to UTC and made naive."""
    # 14:20:24+03:00 should become 11:20:24
    data = {
        "state": {"Vehicle": {"Drivetrain": {"Odometer": 12345}}},
    }
    field_timestamps = {
        "state.Vehicle.Drivetrain.Odometer": "2026-01-05T14:20:24+03:00"
    }

    kafka_producer.redis.get = AsyncMock(return_value=None)

    await kafka_producer.send_filtered_data(
        MakeEnum.kia, data, "VIN123", field_timestamps=field_timestamps
    )

    call_args = kafka_producer.producer.send.call_args
    message = call_args.kwargs["value"]
    assert message["created_at"] == "2026-01-05T11:20:24"

    # Test negative offset: 10:00:00-05:00 should become 15:00:00
    field_timestamps = {
        "state.Vehicle.Drivetrain.Odometer": "2026-01-05T10:00:00-05:00"
    }
    await kafka_producer.send_filtered_data(
        MakeEnum.kia, data, "VIN123", field_timestamps=field_timestamps
    )
    message = kafka_producer.producer.send.call_args.kwargs["value"]
    assert message["created_at"] == "2026-01-05T15:00:00"
