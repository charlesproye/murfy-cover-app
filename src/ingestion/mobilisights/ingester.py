import concurrent.futures
import json
import logging
import os
import signal
import time
from collections.abc import Callable
from datetime import datetime
from queue import Empty, Queue
from types import FrameType
from typing import Any

import boto3
import dotenv
import msgspec
import schedule
from botocore.credentials import threading
from botocore.exceptions import ClientError

from ingestion.ingestion_cache import IngestionCache
from ingestion.mobilisights.api import MSApi
from ingestion.mobilisights.schema import CarState, ErrorMesage


class MobilisightsIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: MSApi
    __s3 = boto3.client("s3")
    __bucket: str

    __fetch_scheduler = schedule.Scheduler()
    __worker_thread: threading.Thread
    __executor: concurrent.futures.ThreadPoolExecutor
    __job_queue: Queue[Callable]

    __shutdown_requested = threading.Event()

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
        MS_FLEET_ID = os.getenv("MS_FLEET_ID")
        if MS_FLEET_ID is None:
            self.__ingester_logger.error("MS_FLEET_ID environment variable not found")
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
        self.__api = MSApi(MS_BASE_URL, MS_EMAIL, MS_PASSWORD, MS_FLEET_ID, MS_COMPANY)
        self.__s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            config=boto3.session.Config(
                signature_version="s3", s3={"addressing_style": "path"}
            ),
        )
        self.__bucket = S3_BUCKET
        self.__executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.__job_queue = Queue()
        self.rate_limit = rate_limit
        self.max_workers = max_workers or self.max_workers

        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)

    def __handle_shutdown_signal(self, signum: int, _frame: FrameType | None):
        self.__ingester_logger.warn(
            f"Received signal {signal.Signals(signum).name}, shutting down"
        )
        self.__shutdown_requested.set()

    def __shutdown(self):
        self.__worker_thread.join()
        self.__ingester_logger.info("Worker thread stopped")
        self.__fetch_scheduler.clear()
        self.__ingester_logger.info("Canceled all jobs")
        self.__executor.shutdown(wait=True, cancel_futures=True)
        self.__ingester_logger.info("Cleared threadpool")
        self.__ingester_logger.info("Main thread stopped")

    def __process_job_queue(self):
        self.__ingester_logger.info("Starting processing job queue")
        while not self.__shutdown_requested.is_set():
            try:
                # Avoid get_nowait to prevent using 100% CPU
                job = self.__job_queue.get(timeout=0.1)
                self.__executor.submit(job)
                self.__job_queue.task_done()
            except Empty:
                pass
        self.__ingester_logger.info("Stopping worker thread")
        return

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

        if self.__ingestion_cache.json_in_db(vin=car_state.vin, json_data=json_data):
            self.__ingester_logger.debug(
                f"[{car_state.vin}] Car state already in cache"
            )
            return

        self.__ingestion_cache.set_json_in_db(vin=car_state.vin, json_data=json_data)
        self.__ingester_logger.info(f"[{car_state.vin}] Successfully parsed data")

        return car_state

    def __process_vehicle(self, data: Any):
        car_state = self._parse_car_state_with_cache(data)

        if car_state is None:
            # If None, the car state is already in the cache or there was an error parsing the data
            return

        filename = f"response/stellantis/{car_state.vin}/temp/{int(datetime.now().timestamp())}.json"

        try:
            uploaded = self.__s3.put_object(
                Body=msgspec.json.encode(car_state),
                Bucket=self.__bucket,
                Key=filename,
            )
            match uploaded["ResponseMetadata"]["HTTPStatusCode"]:
                case 200:
                    self.__ingester_logger.info(
                        f"Uploaded info for vehicle with VIN {car_state.vin} at location {filename}"
                    )
                case _:
                    self.__ingester_logger.error(
                        f"Error uploading info for vehicle with VIN {car_state.vin}: {uploaded['Error']['Message']}"
                    )
                    return
        except ClientError as e:
            self.__ingester_logger.error(
                f"Error uploading info vehicle with VIN {car_state.vin}: {e.response['Error']['Message']}"
            )

    def __fetch_vehicles(self):
        code, res = self.__api.export_car_info()
        match code:
            case 200:
                self.__ingester_logger.info("Fetched vehicle data successfully")
                for chunk in res:
                    if not (chunk == b"[" or chunk == b"]" or chunk == b","):
                        self.__job_queue.put(
                            lambda data=chunk: self.__process_vehicle(data)
                        )
            case 400 | 500:
                for chunk in res:
                    error = msgspec.json.decode(chunk, type=ErrorMesage)
                    self.__ingester_logger.error(
                        f"Encountered error {error.name} while fetching vehicle data: {error.message}"
                    )

    def __schedule_tasks(self):
        self.__fetch_scheduler.every(self.rate_limit).seconds.do(
            self.__job_queue.put, self.__fetch_vehicles
        ).tag("fetch")
        self.__scheduler_logger.info(
            f"Scheduled vehicle fetch every {self.rate_limit} seconds"
        )

        # Run initial fetch
        self.__scheduler_logger.info("Starting initial fetch")
        self.__fetch_vehicles()

    def run(self):
        self.__schedule_tasks()
        self.__worker_thread = threading.Thread(target=self.__process_job_queue)
        self.__ingester_logger.info("Starting worker thread")
        self.__worker_thread.start()
        self.__scheduler_logger.info("Starting scheduler")

        while not self.__shutdown_requested.is_set():
            self.__fetch_scheduler.run_pending()
            time.sleep(1)

        self.__shutdown()

