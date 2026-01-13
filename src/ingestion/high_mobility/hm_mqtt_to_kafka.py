"""
MQTT v2 -> Kafka ingester for High Mobility vehicle data.

Consumes MQTT v2 payloads and forwards filtered data to Kafka in the same
format as hm_kafka_ingester.py.

Implements High Mobility MQTT v2 specification:
https://docs.high-mobility.com/docs/vehicle-data/mqtt-v2
"""

import asyncio
import logging
import os
import signal
import ssl
from pathlib import Path
from types import FrameType
from typing import Any

import aiomqtt
import dotenv
import orjson

from core import WMI_TO_OEM
from core.models import MakeEnum
from ingestion.conversion_utils import get_nested_value
from ingestion.high_mobility import oem_parsers
from ingestion.kafka_producer import KafkaProducerService

logging.getLogger("httpx").setLevel(logging.WARNING)

LOGGER = logging.getLogger(__name__)


class HMMqttToKafkaIngester:
    """Async MQTT v2 ingester for High Mobility vehicle data."""

    def __init__(self, max_concurrency: int = 20):
        """
        Initialize the MQTT v2 -> Kafka ingester.

        Args:
            max_concurrency: Max concurrent message handlers. Defaults to 20.
        """
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency

        # Load MQTT v2 configuration
        # Broker: mqtt-v2.high-mobility.com:8883
        self.mqtt_host = os.getenv("HM_MQTT_HOST", "mqtt-v2.high-mobility.com")
        self.mqtt_port = int(os.getenv("HM_MQTT_PORT", "8883"))

        # MQTT v2 uses QoS 1 for at-least-once delivery
        self.mqtt_qos = 1

        # Application ID is required for MQTT v2
        self.application_id = os.environ["HM_APPLICATION_ID"]

        # MQTT v2 client ID format: stable ID for persistent session
        self.client_id = f"{self.application_id}-{os.getenv('HM_MQTT_CLIENT_SUFFIX', 'bib-ingester')}"

        # Environment: 'live' or 'sandbox'
        self.environment = os.getenv("HM_MQTT_ENV", "live")

        # MQTT v2 topic with shared subscription:
        # $share/{share-group}/{environment}/level13/{application_id}
        self.mqtt_topic = f"$share/{self.application_id}/{self.environment}/level13/{self.application_id}"

        # Certificate-based authentication for MQTT v2
        cert_dir = os.getenv("HM_MQTT_CERT_DIR", f"./certs/hm_mqtt2_{self.environment}")
        self.ca_cert = os.path.join(cert_dir, "ca_crt.pem")
        self.client_cert = os.path.join(cert_dir, "client_crt.pem")
        self.client_key = os.path.join(cert_dir, "client_key.pem")

        for cert_file in [self.ca_cert, self.client_cert, self.client_key]:
            if not Path(cert_file).exists():
                raise FileNotFoundError(
                    f"Certificate file not found: {cert_file}. "
                    f"Download certificates from High Mobility dashboard."
                )

        self.kafka_producer = KafkaProducerService()

        self.shutdown_requested = asyncio.Event()

        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum: int, _frame: FrameType | None):
        LOGGER.warning(f"Received signal {signal.Signals(signum).name}, shutting down")
        self.shutdown_requested.set()

    async def _process_message(self, message: aiomqtt.Message) -> None:
        """
        Process a single MQTT v2 message.

        MQTT v2 message format:
        {
            "version": 2,
            "application_id": "uuid",
            "message_id": "uuid",
            "vin": "VIN17CHARACTERS",
            "data": { ... vehicle data ... }
        }

        Args:
            payload: MQTT v2 message payload
        """
        try:
            payload = orjson.loads(message.payload)

            # Validate MQTT v2 message structure
            if payload.get("version") != 2:
                LOGGER.warning(f"Unsupported message version: {payload.get('version')}")
                return

            vin = payload["vin"]

            json_data = payload["data"]
            brand = self._determine_brand(json_data, vin)

            # Normalize v2 data (convert single-element lists to objects)
            json_data = self._normalize_v2_data(json_data)

            match brand:
                case MakeEnum.renault:
                    json_data = oem_parsers.process_renault(vin, json_data)
                case MakeEnum.mercedes_benz:
                    json_data = oem_parsers.process_mercedes_benz(vin, json_data)
                case MakeEnum.ford:
                    json_data = oem_parsers.process_ford(vin, json_data)
                case MakeEnum.volvo_cars:
                    json_data = oem_parsers.process_volvo_cars(vin, json_data)
                case _:
                    LOGGER.error(f"[{brand}:{vin}] Unsupported brand: {brand}")
                    return

            # Extract field timestamps (High Mobility structure: parent.timestamp for parent.data)
            field_map = self.kafka_producer.field_filter.get_filter(brand)

            field_timestamps = {}
            # Extract field timestamps from the JSON data
            for field_path in field_map:
                parts = field_path.split(".")
                if "data" in parts:
                    parent_path = ".".join(parts[: parts.index("data")])
                    ts_path = f"{parent_path}.timestamp"
                    field_timestamps[field_path] = get_nested_value(json_data, ts_path)

            await self.kafka_producer.send_filtered_data(
                oem=brand,
                data=json_data,
                vin=vin,
                field_timestamps=field_timestamps,
            )

        except Exception as e:
            LOGGER.error(f"Unexpected error processing message: {e}", exc_info=True)

    def _determine_brand(self, data: dict, vin: str) -> MakeEnum:
        """
        Determine vehicle brand from VIN prefix using WMI mapping.

        Args:
            data: Vehicle data (unused, for compatibility)
            vin: Vehicle identification number

        Returns:
            MakeEnum if brand can be determined, None otherwise
        """
        wmi = vin[:3].upper()
        oem = WMI_TO_OEM.get(wmi)
        if not oem:
            raise ValueError(f"Unknown WMI: {wmi}")
        return MakeEnum(oem)

    def _normalize_v2_data(self, data: Any) -> Any:
        """
        Recursively normalize MQTT v2 data.
        Converts single-element lists to the element itself to match
        the structure expected by existing filters and processing logic.
        """
        if isinstance(data, list):
            if len(data) > 0:
                # Take the first element (usually contains the latest state in v2)
                return self._normalize_v2_data(data[0])
            return None
        elif isinstance(data, dict):
            return {k: self._normalize_v2_data(v) for k, v in data.items()}
        return data

    async def run(self):
        """Main entry point for the ingester."""
        LOGGER.info("Starting High Mobility MQTT v2 -> Kafka Ingester")

        try:
            await self.kafka_producer.start()
            LOGGER.info("Kafka producer started")

            LOGGER.info(
                f"Connecting to {self.mqtt_host}:{self.mqtt_port} (topic: {self.mqtt_topic})"
            )

            tls_context = ssl.create_default_context(cafile=self.ca_cert)
            tls_context.load_cert_chain(
                certfile=self.client_cert, keyfile=self.client_key
            )
            # Use persistent session to queue messages during downtime
            async with aiomqtt.Client(
                hostname=self.mqtt_host,
                port=self.mqtt_port,
                identifier=self.client_id,
                tls_context=tls_context,
                protocol=aiomqtt.ProtocolVersion.V5,
                # clean_start=False,  # Resume previous session (MQTT v5)
            ) as client:
                await client.subscribe(self.mqtt_topic, qos=self.mqtt_qos)
                LOGGER.info(
                    f"Connected and subscribed (QoS {self.mqtt_qos}, client ID: {self.client_id})"
                )

                # Semaphore to limit concurrent message processing
                semaphore = asyncio.Semaphore(self.max_concurrency)

                async def handle_with_limit(message: aiomqtt.Message):
                    async with semaphore:
                        await self._process_message(message)

                # Track active tasks
                tasks = set()

                n_messages = 0
                try:
                    async for message in client.messages:
                        if self.shutdown_requested.is_set():
                            LOGGER.info("Shutdown requested, stopping...")
                            break

                        task = asyncio.create_task(handle_with_limit(message))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)

                        n_messages += 1
                        if n_messages % 100 == 0:
                            LOGGER.info(f"Processed {n_messages} messages")

                except Exception as e:
                    LOGGER.error(f"Error in message loop: {e}", exc_info=True)

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                LOGGER.info("All messages processed")

        except Exception as e:
            LOGGER.error(f"MQTT error: {e}", exc_info=True)
        finally:
            await self.kafka_producer.stop()
            LOGGER.info("Shutdown complete")


def main():
    """CLI entry point."""
    dotenv.load_dotenv()
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    ingester = HMMqttToKafkaIngester()
    asyncio.run(ingester.run())


if __name__ == "__main__":
    main()
