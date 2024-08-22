import concurrent.futures
import logging
import os
import time
from datetime import datetime
from queue import Queue
from typing import Any, Callable, Optional

import boto3
import dotenv
import msgspec
import schedule
from botocore.credentials import threading
from ingestion.mobilisights.api import MSApi
from ingestion.mobilisights.schema import CarState, ErrorMesage


class MobilisightsIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: MSApi
    __s3 = boto3.client("s3")
    __bucket: str

    __fetch_scheduler = schedule.Scheduler()
    __compress_scheduler = schedule.Scheduler()
    __executor: concurrent.futures.ThreadPoolExecutor
    __job_queue: Queue[Callable]

    __json_encoder = msgspec.json.Encoder()
    __encode_buffer = bytearray()

    def __encode(self, obj):
        self.__json_encoder.encode_into(obj, self.__encode_buffer)
        return self.__encode_buffer

    rate_limit: int = 36
    upload_interval: int = 60
    compress_interval: int = 12
    max_workers: int = 8
    compress_threaded: bool = True

    def __init__(
        self,
        rate_limit: Optional[int] = 36,
        max_workers: Optional[int] = 8,
        compress_interval: Optional[int] = 12,
        compress_threaded: Optional[bool] = True,
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
        compress_interval: int, optional
            The interval at which to compress the S3 data (in hours)
            default: 12
        """
        dotenv.load_dotenv()
        MS_BASE_URL = os.getenv("MS_BASE_URL")
        if MS_BASE_URL is None:
            self.__ingester_logger.error("MS_BASE_URL environment variable not found")
            return
        MS_ACCESS_TOKEN = os.getenv("MS_ACCESS_TOKEN")
        if MS_ACCESS_TOKEN is None:
            self.__ingester_logger.error(
                "MS_ACCESS_TOKEN environment variable not found"
            )
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
        self.__api = MSApi(MS_BASE_URL, MS_ACCESS_TOKEN, MS_FLEET_ID, MS_COMPANY)
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
        self.compress_interval = compress_interval or self.compress_interval
        self.max_workers = max_workers or self.max_workers
        self.compress_threaded = compress_threaded or self.compress_threaded

        self.__ingester_logger = logging.getLogger("INGESTER")
        self.__scheduler_logger = logging.getLogger("SCHEDULER")

    def __process_job_queue(self):
        self.__ingester_logger.info("Starting processing job queue")
        while 1:
            job = self.__job_queue.get()
            self.__executor.submit(job)
            self.__job_queue.task_done()

    def __process_vehicle(self, data: Any):
        try:
            car_state = msgspec.json.decode(data, type=CarState)
        except msgspec.ValidationError as e:
            self.__ingester_logger.error(f"Failed to parse car data: {e}")
            return
        self.__ingester_logger.info(f"Parsed data for VIN {car_state.vin} successfully")
        filename = f"response/stellantis/{car_state.vin}/temp/{int(datetime.now().timestamp())}.json"
        encoded = self.__encode(car_state)
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
                    f"Error uploading info for vehicle with vin {car_state.vin}: {uploaded}"
                )
        return

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

    def run(self):
        worker_thread = threading.Thread(target=self.__process_job_queue)
        self.__scheduler_logger.info(
            f"Scheduling data fetching every {self.rate_limit} seconds"
        )
        self.__fetch_scheduler.every(self.rate_limit).seconds.do(
            self.__job_queue.put, self.__fetch_vehicles
        )
        self.__scheduler_logger.info("Running initial scheduled tasks")
        self.__fetch_scheduler.run_all()
        self.__ingester_logger.info("Starting worker thread")
        worker_thread.start()
        self.__scheduler_logger.info("Starting scheduler")
        while 1:
            now = datetime.now().hour
            if now >= 6 and now <= 23:
                self.__fetch_scheduler.run_pending()
            else:
                self.__compress_scheduler.run_pending()
            time.sleep(1)

