import concurrent.futures
import logging
import os
import signal
import time
from datetime import datetime
from queue import Empty, Queue
from types import FrameType
from typing import Any, Callable, Optional

import boto3
import dotenv
import msgspec
import schedule
from botocore.credentials import threading
from botocore.exceptions import ClientError
from ingestion.mobilisights.api import MSApi
from ingestion.mobilisights.compresser import MobilisightsCompresser
from ingestion.mobilisights.schema import CarState, ErrorMesage


class MobilisightsIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: MSApi
    __s3 = boto3.client("s3")
    __bucket: str

    __fetch_scheduler = schedule.Scheduler()
    __compress_scheduler = schedule.Scheduler()
    __worker_thread: threading.Thread
    __executor: concurrent.futures.ThreadPoolExecutor
    __job_queue: Queue[Callable]

    __shutdown_requested = threading.Event()

    rate_limit: int = 36
    upload_interval: int = 60
    compress_time: str = "00"
    max_workers: int = 8
    compress_threaded: bool = True

    def __init__(
        self,
        rate_limit: Optional[int] = 36,
        max_workers: Optional[int] = 8,
        compress_time: Optional[str] = "00:00",
    ):
        """
        Parameters
        ----------
        refresh_interval: int, optional
            The interval at wich to update the vehicle list (in minutes)
            default: 120
        max_workers: int, optional
            The maximum numbers of workers (limited by the S3 bucket options)
            default: 8
        compress_time: str, optional
            The time of day at which to compress the S3 data
            default: "00:00"
        """
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
        )
        self.__bucket = S3_BUCKET
        self.__executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.__job_queue = Queue()
        self.rate_limit = rate_limit or self.rate_limit
        self.compress_time = compress_time or self.compress_time
        self.max_workers = max_workers or self.max_workers

        self.__ingester_logger = logging.getLogger("INGESTER")
        self.__scheduler_logger = logging.getLogger("SCHEDULER")

        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)

    def __handle_shutdown_signal(self, signum: int, _frame: Optional[FrameType]):
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
                job = self.__job_queue.get_nowait()
                self.__executor.submit(job)
                self.__job_queue.task_done()
            except Empty:
                pass
        self.__ingester_logger.info("Stopping worker thread")
        return

    def __process_vehicle(self, data: Any):
        try:
            car_state = msgspec.json.decode(data, type=CarState)
        except (msgspec.ValidationError, msgspec.DecodeError) as e:
            self.__ingester_logger.error(f"Failed to parse car data: {e}")
            return
        self.__ingester_logger.info(f"Parsed data for VIN {car_state.vin} successfully")
        filename = f"response/stellantis/{car_state.vin}/temp/{int(datetime.now().timestamp())}.json"
        try:
            encoded = msgspec.json.encode(car_state)
        except msgspec.EncodeError as e:
            self.__ingester_logger.error(f"Failed to reencode car data: {e}")
            return
        try:
            uploaded = self.__s3.put_object(
                Body=encoded,
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

    def __compress(self):
        self.__ingester_logger.info("Starting compression job")
        compresser = MobilisightsCompresser(
            self.__s3,
            self.__bucket,
            threaded=False,
            max_workers=self.max_workers,
        )
        compresser.run()

    def run(self):
        # Check for a forced compression parameter
        compress_only = os.getenv("COMPRESS_ONLY") == "1"
        
        if compress_only:
            self.__ingester_logger.info("COMPRESS_ONLY flag set. Running compression first.")
            self.__is_compressing = True
            try:
                self.__compress()
            except Exception as e:
                self.__ingester_logger.error(f"Error during compression: {e}")
            finally:
                self.__is_compressing = False
            self.__ingester_logger.info("Compression completed. Exiting.")
            return

        self.__schedule_tasks()
        self.__worker_thread = threading.Thread(target=self.__process_job_queue)
        self.__ingester_logger.info("Starting worker thread")
        self.__worker_thread.start()
        self.__scheduler_logger.info("Starting scheduler")

        while not self.__shutdown_requested.is_set():
            if not self.__is_compressing:
                self.__fetch_scheduler.run_pending()
            self.__compress_scheduler.run_pending()
            time.sleep(1)

        self.__shutdown()

