"""
Kafka producer service for pushing filtered OEM data.

Filters fields based on CSV files in src/ingestion/data/ and sends
to Kafka in the format: (vin, metric_name, created_at, value_int, value_double, value_string, value_bool)
"""

import asyncio
import csv
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, cast

import msgspec
from aiokafka import AIOKafkaProducer
from fastapi import Depends
from redis.asyncio import Redis

from core.models import MakeEnum
from ingestion.push.config import KafkaSettings, RedisSettings

LOGGER = logging.getLogger(__name__)


class FieldFilter:
    """Loads and caches field filters from CSV files."""

    def __init__(self):
        self._filters: dict[str, dict[str, str]] = {}
        self._data_dir = Path(__file__).parent / "data"

    def load_all_filters(self) -> None:
        """Pre-load all OEM filters from CSV files at startup."""
        if not self._data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self._data_dir}")

        # Load all CSV files in the data directory
        for csv_file in self._data_dir.glob("*.csv"):
            oem = csv_file.stem
            field_map = {}

            with open(csv_file) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    field_name = row.get("field_name", "").strip()
                    field_type = row.get("field_type", "").strip()
                    if field_name and field_type:
                        field_map[field_name] = field_type

            self._filters[oem] = field_map
            LOGGER.info(f"Loaded {len(field_map)} fields for {oem}")

    def get_filter(self, oem: MakeEnum) -> dict[str, str]:
        """
        Get field filter for an OEM.

        Args:
            oem: MakeEnum

        Returns:
            Dict mapping field_name to field_type
        """
        return self._filters.get(oem, {})


class KafkaProducerService:
    """Service for producing filtered OEM data to Kafka."""

    def __init__(self):
        self.settings = KafkaSettings()
        self.redis_settings = RedisSettings()
        self.producer: AIOKafkaProducer | None = None
        self.redis: Redis | None = None
        self.field_filter = FieldFilter()
        self._start_lock = asyncio.Lock()

    async def start(self):
        """Initialize Kafka producer, Redis client, and load field filters."""
        async with self._start_lock:
            if self.producer is None:
                self.field_filter.load_all_filters()

                producer = AIOKafkaProducer(
                    bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: msgspec.json.encode(v),
                )
                await producer.start()
                self.producer = producer
                LOGGER.info(
                    f"Kafka producer started for {self.settings.KAFKA_PUSH_TOPIC} on {self.settings.KAFKA_BOOTSTRAP_SERVERS}"
                )

            if self.redis is None:
                # Initialize async Redis client for field-level caching (store bytes directly)
                self.redis = Redis(
                    host=self.redis_settings.REDIS_HOST,
                    port=self.redis_settings.REDIS_PORT,
                    username=self.redis_settings.REDIS_USER,
                    password=self.redis_settings.REDIS_PASSWORD,
                    decode_responses=False,  # Store raw bytes for efficiency
                    db=self.redis_settings.REDIS_DB_KAFKA_CACHE,
                )
                LOGGER.info(
                    f"Redis cache initialized for Kafka: db={self.redis_settings.REDIS_DB_KAFKA_CACHE}"
                )

    async def stop(self):
        """Stop Kafka producer and close Redis connection."""
        async with self._start_lock:
            if self.producer:
                await self.producer.stop()
                self.producer = None
                LOGGER.info("Kafka producer stopped")

            if self.redis:
                await self.redis.aclose()
                self.redis = None
                LOGGER.info("Redis connection closed")

    def flatten_dict(self, data: dict, parent_key: str = "") -> dict[str, Any]:
        """
        Flatten nested dictionary using dot notation.

        Args:
            data: Nested dictionary
            parent_key: Parent key prefix

        Returns:
            Flattened dictionary
        """
        items = {}
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, dict):
                items.update(self.flatten_dict(value, new_key))
            else:
                items[new_key] = value

        return items

    def create_kafka_message(
        self, field_name: str, field_type: str, value: Any, vin: str, created_at: str
    ) -> dict:
        """
        Create Kafka message with typed value fields.

        Args:
            field_name: Metric name
            field_type: Type (int, double, string, bool)
            value: Value to convert
            vin: Vehicle identification number
            created_at: Timestamp (should be naive UTC ISO format)

        Returns:
            Message dict with typed value fields
        """
        message = {
            "vin": vin,
            "metric_name": field_name,
            "created_at": created_at,
            "value_int": None,
            "value_double": None,
            "value_string": None,
            "value_bool": None,
        }

        if field_type == "int":
            message["value_int"] = int(value) if value is not None else None
        elif field_type == "double":
            message["value_double"] = float(value) if value is not None else None
        elif field_type == "bool":
            if isinstance(value, bool):
                message["value_bool"] = value
            elif isinstance(value, str):
                message["value_bool"] = value.lower() in ("true", "1", "yes", "on")
            else:
                message["value_bool"] = bool(value) if value is not None else None
        else:  # string or unknown type
            message["value_string"] = str(value) if value is not None else None

        return message

    async def has_value_changed(
        self, oem: MakeEnum, vin: str, field_name: str, value: Any
    ) -> bool:
        """
        Check if a field value has changed since last send.

        Args:
            oem: OEM name
            vin: Vehicle identification number
            field_name: Field/metric name
            value: Current value

        Returns:
            True if value changed or is new, False if unchanged
        """
        if self.redis is None:
            await self.start()
            if self.redis is None:
                raise RuntimeError("Redis client failed to initialize")

        # Type narrowing: after the check above, self.redis is guaranteed to be Redis
        redis = cast(Redis, self.redis)
        cache_key = f"kafka_push:{oem}:{vin}:{field_name}"

        current_value_bytes = msgspec.json.encode(value)

        cached_value_bytes: bytes | None = await redis.get(cache_key)

        if cached_value_bytes == current_value_bytes:
            return False

        await redis.set(cache_key, current_value_bytes)
        return True

    def _normalize_datetime(self, dt_val: datetime | str) -> str:
        """
        Normalize datetime to UTC and remove timezone info for Kafka.
        Kafka/Iceberg expects timestamps without TZ offsets (naive UTC).

        Args:
            dt_val: datetime object or ISO format string

        Returns:
            ISO format string without TZ info
        """
        if isinstance(dt_val, str):
            # Fast path 1: Already naive ISO-like format (no Z, no +, no offset hyphen)
            if "Z" not in dt_val and "+" not in dt_val and dt_val.count("-") <= 2:
                return dt_val

            # Fast path 2: UTC indicator 'Z' at the end
            if dt_val.endswith("Z"):
                return dt_val[:-1]

            # Fast path 3: Explicit UTC offset '+00:00'
            if dt_val.endswith("+00:00"):
                return dt_val[:-6]

            # If it has a non-UTC offset or complex format, we must parse it to convert to UTC
            try:
                # fromisoformat handles +HH:MM, and in 3.11+ it handles Z
                dt = datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
            except ValueError:
                # Fallback for weird formats: strip any TZ offset if present to avoid parsing errors
                if "+" in dt_val:
                    return dt_val.split("+")[0]
                return dt_val
        else:
            dt = dt_val

        if dt.tzinfo is not None:
            # Convert to UTC and then make naive
            dt = dt.astimezone(UTC).replace(tzinfo=None)

        return dt.isoformat()

    async def send_filtered_data(
        self,
        oem: MakeEnum,
        data: dict,
        vin: str,
        message_datetime: str | None = None,
        field_timestamps: dict[str, str] | None = None,
    ) -> int:
        """
        Filter and send data to Kafka.

        Args:
            oem: MakeEnum
            data: Raw JSON data
            vin: VIN
            message_datetime: Optional datetime for the message, must be UTC naive ISO format
            field_timestamps: Optional mapping of field_name -> timestamp for field-specific timestamps

        Returns:
            Number of messages sent
        """
        if self.producer is None:
            await self.start()

        field_map = self.field_filter.get_filter(oem)
        if not field_map:
            LOGGER.warning(f"No fields configured for {oem.value}, skipping Kafka send")
            return 0

        if message_datetime is None:
            message_datetime = datetime.now(UTC).replace(tzinfo=None).isoformat()

        message_datetime_str = self._normalize_datetime(message_datetime)

        flattened = self.flatten_dict(data)

        messages_sent = 0
        for field_name, field_type in field_map.items():
            if field_name in flattened:
                value = flattened[field_name]

                if not await self.has_value_changed(oem, vin, field_name, value):
                    continue

                # Use field-specific timestamp if provided, otherwise use default
                raw_field_ts = (
                    field_timestamps.get(field_name) if field_timestamps else None
                )
                field_created_at = (
                    self._normalize_datetime(raw_field_ts)
                    if raw_field_ts
                    else message_datetime_str
                )

                try:
                    message = self.create_kafka_message(
                        field_name, field_type, value, vin, field_created_at
                    )
                except (ValueError, TypeError) as e:
                    LOGGER.error(
                        f"Error creating Kafka message for {field_name}={value} (type={type(value).__name__}, expected={field_type}): {e}"
                    )
                    continue

                try:
                    if self.producer:
                        await self.producer.send(
                            topic=self.settings.KAFKA_PUSH_TOPIC + "_" + oem.value,
                            key=vin.encode("utf-8"),  # Key must be bytes
                            value=message,
                        )
                        messages_sent += 1
                except Exception as e:
                    LOGGER.error(f"Error sending {field_name}={value} to Kafka: {e}")

        if messages_sent > 0:
            LOGGER.debug(
                f"Sent {messages_sent} messages to Kafka for {oem.value} VIN {vin}"
            )

        return messages_sent


# Global producer instance and lock
_kafka_producer: KafkaProducerService | None = None
_kafka_producer_lock = asyncio.Lock()


async def get_kafka_producer() -> KafkaProducerService:
    """
    Get or create global Kafka producer instance.

    Returns:
        KafkaProducerService instance
    """
    global _kafka_producer
    if _kafka_producer is None:
        async with _kafka_producer_lock:
            if _kafka_producer is None:
                producer = KafkaProducerService()
                await producer.start()
                _kafka_producer = producer
    assert _kafka_producer is not None
    return _kafka_producer


async def stop_kafka_producer() -> None:
    """
    Stop the global Kafka producer instance if it exists.
    """
    global _kafka_producer
    async with _kafka_producer_lock:
        if _kafka_producer is not None:
            await _kafka_producer.stop()
            _kafka_producer = None


KafkaProducerDep = Annotated[KafkaProducerService, Depends(get_kafka_producer)]
