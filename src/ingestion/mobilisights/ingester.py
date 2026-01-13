import asyncio
import json
import logging
import os
import signal
from datetime import datetime
from types import FrameType
from typing import Any

import dotenv
import msgspec
import orjson
from botocore.client import ClientError

from core.models import MakeEnum
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from ingestion.ingestion_cache import IngestionCache
from ingestion.kafka_producer import KafkaProducerService
from ingestion.mobilisights.api import MSApi
from ingestion.mobilisights.ms_utils import convert_json_units
from ingestion.mobilisights.schema import CarState, ErrorMesage


class MobilisightsIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: MSApi
    __s3: S3Service
    __bucket: str
    __kafka_producer: KafkaProducerService

    __shutdown_requested: asyncio.Event

    upload_interval: int = 60
    max_workers: int = 8

    def __init__(self, rate_limit: int = 60, max_workers: int | None = 8):
        """
        Parameters
        ----------
        refresh_interval: int, optional
            The interval at wich to update the vehicle list (in minutes)
            default: 120
        max_workers: int, optional
            The maximum numbers of workers (limited by the S3 bucket options)
            default: 8
        """
        self.__ingester_logger = logging.getLogger("INGESTER")
        self.__scheduler_logger = logging.getLogger("SCHEDULER")
        self.__is_compressing = False
        self.__ingestion_cache: IngestionCache = IngestionCache(
            make="mobilisights", keys_to_ignore=["_id", "datetimeSending", "*.datetime"]
        )
        self.__decoder = msgspec.json.Decoder(CarState)

        dotenv.load_dotenv()
        MS_BASE_URL = os.getenv("MS_BASE_URL")
        if MS_BASE_URL is None:
            self.__ingester_logger.error("MS_BASE_URL environment variable not found")
            return
        MS_EMAIL = os.getenv("MS_EMAIL")
        if MS_EMAIL is None:
            self.__ingester_logger.error("MS_EMAIL environment variable not found")
            return
        MS_PASSWORD = os.getenv("MS_PASSWORD")
        if MS_PASSWORD is None:
            self.__ingester_logger.error("MS_PASSWORD environment variable not found")
            return
        MS_COMPANY = os.getenv("MS_COMPANY")
        if MS_COMPANY is None:
            self.__ingester_logger.error("MS_COMPANY environment variable not found")
            return
        S3_ENDPOINT = os.getenv("S3_ENDPOINT")
        if S3_ENDPOINT is None:
            self.__ingester_logger.error("S3_ENDPOINT environment variable not found")
            return
        S3_REGION = os.getenv("S3_REGION")
        if S3_REGION is None:
            self.__ingester_logger.error("S3_REGION environment variable not found")
            return
        S3_BUCKET = os.getenv("S3_BUCKET")
        if S3_BUCKET is None:
            self.__ingester_logger.error("S3_BUCKET environment variable not found")
            return
        S3_KEY = os.getenv("S3_KEY")
        if S3_KEY is None:
            self.__ingester_logger.error("S3_KEY environment variable not found")
            return
        S3_SECRET = os.getenv("S3_SECRET")
        if S3_SECRET is None:
            self.__ingester_logger.error("S3_SECRET environment variable not found")
            return
        self.__api = MSApi(MS_BASE_URL, MS_EMAIL, MS_PASSWORD, MS_COMPANY)

        s3_settings = S3Settings(
            S3_ENDPOINT=S3_ENDPOINT,
            S3_REGION=S3_REGION,
            S3_BUCKET=S3_BUCKET,
            S3_KEY=S3_KEY,
            S3_SECRET=S3_SECRET,
        )
        self.__s3: S3Service = S3Service(s3_settings)
        self.__bucket = S3_BUCKET
        self.__job_queue = asyncio.Queue()
        self.__shutdown_requested = asyncio.Event()
        self.rate_limit = rate_limit
        self.max_workers = max_workers or self.max_workers

        # Kafka producer will be initialized in async context
        self.__kafka_producer = KafkaProducerService()

        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)

    def __handle_shutdown_signal(self, signum: int, _frame: FrameType | None):
        self.__ingester_logger.warning(
            f"Received signal {signal.Signals(signum).name}, shutting down"
        )
        self.__shutdown_requested.set()

    async def __shutdown(self):
        self.__ingester_logger.info("Shutting down...")

        # Stop Kafka producer
        await self.__kafka_producer.stop()
        self.__ingester_logger.info("Kafka producer stopped")

        self.__ingester_logger.info("Shutdown complete")

    async def __process_job_queue(self):
        """Process jobs from the queue with max_workers concurrency."""
        self.__ingester_logger.info("Starting job queue processor")
        tasks = set()

        while not self.__shutdown_requested.is_set():
            # Maintain max_workers concurrent tasks
            while len(tasks) < self.max_workers and not self.__job_queue.empty():
                try:
                    job = await asyncio.wait_for(self.__job_queue.get(), timeout=0.1)
                    task = asyncio.create_task(job())
                    tasks.add(task)
                except TimeoutError:
                    break

            # Wait a bit before checking again
            await asyncio.sleep(0.1)

            # Clean up completed tasks
            tasks = {t for t in tasks if not t.done()}

        # Wait for remaining tasks to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self.__ingester_logger.info("Job queue processor stopped")

    def _parse_car_state_with_cache(self, data: bytes) -> CarState | None:
        json_data = json.loads(data)

        try:
            car_state = self.__decoder.decode(data)
        except (msgspec.ValidationError, msgspec.DecodeError) as e:
            vin = json_data.get("vin")
            self.__ingester_logger.error(
                f"[{vin}] Failed to parse car data: {e}. Data: {data}"
            )
            return

        if not car_state.vin:
            self.__ingester_logger.error("Car state missing VIN, skipping")
            return

        if self.__ingestion_cache.json_in_db(vin=car_state.vin, json_data=json_data):
            self.__ingester_logger.debug(
                f"[{car_state.vin}] Car state already in cache"
            )
            return

        self.__ingestion_cache.set_json_in_db(vin=car_state.vin, json_data=json_data)
        self.__ingester_logger.info(f"[{car_state.vin}] Successfully parsed data")

        return car_state

    async def __send_to_kafka(self, json_data: dict) -> None:
        """
        Send car state to Kafka using the Kafka producer service.

        Args:
            json_data: Raw JSON data as dictionary
        """
        if not json_data.get("vin"):
            self.__ingester_logger.warning("Cannot send to Kafka: VIN is missing")
            return

        json_data = convert_json_units(json_data)

        # Send to Kafka
        await self.__kafka_producer.send_filtered_data(
            oem=MakeEnum.stellantis,
            data=json_data,
            vin=json_data["vin"],
            message_datetime=json_data["datetime"].strip("Z"),
        )

    async def __process_vehicle(self, data: Any):
        """Process a single vehicle's data."""
        json_data = orjson.loads(data)
        try:
            await self.__send_to_kafka(json_data)
        except Exception as e:
            self.__ingester_logger.error(f"Error sending to Kafka: {e}")

        car_state = self._parse_car_state_with_cache(data)
        if car_state is None:
            # If None, the car state is already in the cache or there was an error parsing the data
            return

        filename = f"response/stellantis/{car_state.vin}/temp/{int(datetime.now().timestamp())}.json"

        try:
            self.__s3.store_object(msgspec.json.encode(car_state), filename)
            self.__ingester_logger.info(
                f"Uploaded info for vehicle with VIN {car_state.vin} at location {filename}"
            )
        except ClientError as e:
            self.__ingester_logger.error(
                f"Error uploading info vehicle with VIN {car_state.vin}: {e.response['Error']['Message']}"
            )

    async def __fetch_vehicles(self):
        """Fetch vehicles from API and queue them for processing."""
        code, res = self.__api.export_car_info()
        match code:
            case 200:
                self.__ingester_logger.info("Fetched vehicle data successfully")
                for chunk in res:
                    if not (chunk == b"[" or chunk == b"]" or chunk == b","):
                        # Queue a coroutine for processing
                        await self.__job_queue.put(
                            lambda data=chunk: self.__process_vehicle(data)
                        )
            case 400 | 500:
                for chunk in res:
                    error = msgspec.json.decode(chunk, type=ErrorMesage)
                    self.__ingester_logger.error(
                        f"Encountered error {error.name} while fetching vehicle data: {error.message}"
                    )

    async def __periodic_fetch(self):
        """Periodically fetch vehicles at the configured rate."""
        self.__scheduler_logger.info(
            f"Starting periodic fetch every {self.rate_limit} seconds"
        )

        # Run initial fetch
        self.__scheduler_logger.info("Starting initial fetch")
        await self.__fetch_vehicles()

        # Then fetch periodically
        while not self.__shutdown_requested.is_set():
            await asyncio.sleep(self.rate_limit)
            if not self.__shutdown_requested.is_set():
                await self.__fetch_vehicles()

        self.__scheduler_logger.info("Periodic fetch stopped")

    async def run(self):
        """Main async entry point for the ingester."""
        # Start Kafka producer
        await self.__kafka_producer.start()
        self.__ingester_logger.info("Kafka producer started")

        # Create tasks
        fetch_task = asyncio.create_task(self.__periodic_fetch())
        process_task = asyncio.create_task(self.__process_job_queue())

        self.__ingester_logger.info("Ingester started")

        # Wait for shutdown signal
        await self.__shutdown_requested.wait()

        # Cancel tasks
        fetch_task.cancel()
        process_task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(fetch_task, process_task, return_exceptions=True)

        # Shutdown
        await self.__shutdown()
