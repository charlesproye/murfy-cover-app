import concurrent.futures
import logging
import os
import signal
import threading
import time
from datetime import datetime
from queue import Empty, Queue
from types import FrameType
from typing import Callable, Optional

import boto3
import dotenv
import msgspec
import schedule
from botocore.client import ClientError
from ingestion.high_mobility.api import HMApi
from ingestion.high_mobility.compress_data import HMCompresser
from ingestion.high_mobility.schema import brands
from ingestion.high_mobility.schema.brands import decode_vehicle_info
from ingestion.high_mobility.vehicle import Vehicle


class HMIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: HMApi
    __s3 = boto3.client("s3")
    __bucket: str
    __compresser: HMCompresser

    __fetch_scheduler = schedule.Scheduler()
    __compress_scheduler = schedule.Scheduler()
    __vehicles: set[Vehicle] = set()
    __worker_thread: threading.Thread
    __executor: concurrent.futures.ThreadPoolExecutor
    __job_queue: Queue[Callable]

    __shutdown_requested = threading.Event()

    refresh_interval: int = 2 * 60
    upload_interval: int = 60
    compress_interval: int = 12
    max_workers: int = 8
    compress_threaded: bool = True

    def __init__(
        self,
        refresh_interval: Optional[int] = 2 * 60,
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
        HM_BASE_URL = os.getenv("HM_BASE_URL")
        if HM_BASE_URL is None:
            self.__ingester_logger.error("HM_BASE_URL environment variable not found")
            return
        HM_CLIENT_ID = os.getenv("HM_CLIENT_ID")
        if HM_CLIENT_ID is None:
            self.__ingester_logger.error("HM_CLIENT_ID environment variable not found")
            return
        HM_CLIENT_SECRET = os.getenv("HM_CLIENT_SECRET")
        if HM_CLIENT_SECRET is None:
            self.__ingester_logger.error(
                "HM_CLIENT_SECRET environment variable not found"
            )
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
        self.__api = HMApi(HM_BASE_URL, HM_CLIENT_ID, HM_CLIENT_SECRET)
        self.__s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
        )
        self.__bucket = S3_BUCKET
        self.__executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.__compresser = HMCompresser(
            self.__s3,
            self.__bucket,
            threaded=self.compress_threaded,
            max_workers=self.max_workers,
        )
        self.__job_queue = Queue()
        self.refresh_interval = refresh_interval or self.refresh_interval
        self.compress_interval = compress_interval or self.compress_interval
        self.max_workers = max_workers or self.max_workers
        self.compress_threaded = compress_threaded or self.compress_threaded

        self.__ingester_logger = logging.getLogger("INGESTER")
        self.__scheduler_logger = logging.getLogger("SCHEDULER")
        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)

    def __handle_shutdown_signal(self, signum: int, _frame: Optional[FrameType]):
        self.__ingester_logger.warn(
            f"Received signal {signal.Signals(signum).name}, shutting down"
        )
        self.__request_shutdown()

    def __request_shutdown(self):
        self.__shutdown_requested.set()
        self.__compresser.shutdown()

    def __shutdown(self):
        self.__worker_thread.join()
        self.__ingester_logger.info("Worker thread stopped")
        self.__fetch_scheduler.clear()
        self.__ingester_logger.info("Canceled all jobs")
        self.__executor.shutdown(wait=True, cancel_futures=True)
        self.__ingester_logger.info("Cleared threadpool")
        self.__ingester_logger.info("Main thread stopped")

    def __fetch_clearances(self) -> list[Vehicle] | None:
        error, info = self.__api.list_clearances(status="approved")
        match error:
            case 401:
                self.__ingester_logger.error(
                    "Error fetching vehicles with an approved clearance: Unauthorized"
                )
                return
            case 403:
                self.__ingester_logger.error(
                    "Error fetching vehicles with an approved clearance: Forbidden"
                )
                return
            case 500:
                self.__ingester_logger.error(
                    "Error fetching vehicles with an approved clearance: Server Error"
                )
                return
            case 200:
                self.__ingester_logger.info("Fetched approved clearances successfully")
                if not isinstance(info, list):
                    self.__ingester_logger.error(
                        f"Wrong format for clearance list (expected list, got {type(info)})"
                    )
                    return
                if not isinstance(info[0], Vehicle):
                    self.__ingester_logger.error(
                        f"Wrong format for clearance list elements (expected Vehicle, got {type(info[0])})"
                    )
                    return
                return info
            case _:
                self.__ingester_logger.error(
                    f"Unexpected error while fetching vehicles with an approved clearance: {error}"
                )
                return

    def __update_vehicles_initial(self) -> None:
        clearances = self.__fetch_clearances()
        if clearances is None:
            return
        vehicles = set(
            Vehicle(
                vin=clearance.vin,
                brand=clearance.brand,
                rate_limit=brands[clearance.brand].rate_limit,
                clearance_status=clearance.clearance_status,
            )
            for clearance in clearances
        )
        self.__ingester_logger.info(
            f"Fetched {len(vehicles)} with an approved clearance"
        )
        self.__vehicles.update(vehicles)
        for vehicle in vehicles:
            self.__fetch_scheduler.every(vehicle.rate_limit).seconds.do(
                self.__job_queue.put, lambda v=vehicle: self.__process_vehicle(v)
            ).tag(vehicle.vin)
            self.__scheduler_logger.info(
                f"Adding vehicle with VIN {vehicle.vin} (brand {vehicle.brand}) to the scheduler (interval: {vehicle.rate_limit} seconds)"
            )
        self.__fetch_scheduler.every(self.refresh_interval).minutes.do(
            self.__job_queue.put,
            self.__update_vehicles,
        ).tag("refresh")
        self.__scheduler_logger.info(
            f"Scheduled refresh of vehicle list in {self.refresh_interval} minutes"
        )

    def __update_vehicles(self) -> None:
        clearances = self.__fetch_clearances()
        if clearances is None:
            return
        updated_vehicles = set(
            [
                Vehicle(
                    vin=clearance.vin,
                    brand=clearance.brand,
                    rate_limit=brands[clearance.brand].rate_limit,
                    clearance_status=clearance.clearance_status,
                )
                for clearance in clearances
            ]
        )
        vehicles_to_add = updated_vehicles.difference(self.__vehicles)
        vehicles_to_remove = self.__vehicles.difference(updated_vehicles)
        self.__vehicles.update(vehicles_to_add)
        self.__vehicles.difference_update(vehicles_to_remove)
        self.__ingester_logger.info(
            f"Updating VINs: {len(vehicles_to_remove)} to remove, {len(vehicles_to_add)} to add"
        )
        for v in vehicles_to_add:
            self.__scheduler_logger.info(
                f"Adding vehicle with VIN {v.vin} (brand {v.brand}) to scheduler (interval {v.rate_limit} seconds)"
            )
            self.__fetch_scheduler.every(v.rate_limit).seconds.do(
                self.__executor.submit, lambda vv=v: self.__process_vehicle(vv)
            )
        for v in vehicles_to_remove:
            self.__scheduler_logger.info(
                f"Removing task for vehicle with VIN {v.vin} (brand {v.brand})"
            )
            schedule.clear(v.vin)

    def __process_vehicle(self, vehicle: Vehicle) -> None:
        self.__ingester_logger.info(
            f"Starting processing for vehicle with VIN {vehicle.vin} (brand {vehicle.brand})"
        )
        error, info = self.__api.get_vehicle_info(vehicle.vin)

        def log_error(info):
            self.__ingester_logger.error(
                f"Error getting info for VIN {vehicle.vin} (brand {vehicle.brand}) (request {info['request_id']}): {info['errors'][0]['title']} - {info['errors'][0]['detail']}"
            )

        match error:
            case 200:
                self.__ingester_logger.info(
                    f"Fetched vehicle info for vehicle with VIN {vehicle.vin} (brand {vehicle.brand})"
                )
                try:
                    decoded = decode_vehicle_info(info, vehicle.brand)
                except (msgspec.ValidationError, msgspec.DecodeError) as e:
                    self.__ingester_logger.error(
                        f"Unable to parse vehicle info for vehicle with VIN {vehicle.vin} (brand {vehicle.brand}): response {info} does not fit schema ({e})"
                    )
                    return
                self.__ingester_logger.info(
                    f"Parsed response for vehicle with VIN {vehicle.vin} correctly"
                )
                filename = f"response/{vehicle.brand}/{vehicle.vin}/temp/{int(datetime.now().timestamp())}.json"
                try:
                    encoded = msgspec.json.encode(decoded)
                except msgspec.EncodeError as e:
                    self.__ingester_logger.error(f"Failed to encode vehicle data: {e}")
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
                                f"Uploaded info for vehicle with VIN {vehicle.vin} at location {filename}"
                            )
                        case _:
                            self.__ingester_logger.error(
                                f"Error uploading info for vehicle with VIN {vehicle.vin}: {uploaded['Error']['Message']}"
                            )
                except ClientError as e:
                    self.__ingester_logger.error(
                        f"Error uploading info for vehicle with VIN {vehicle.vin}: {e.response['Error']['Message']}"
                    )
                return
            case _:
                log_error(info)
                return

    def __compress(self):
        self.__ingester_logger.info("Starting compression job")
        self.__compresser.run()

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

    def run(self):
        if os.getenv("COMPRESS_ONLY"):
            self.__compress()
        else:
            self.__update_vehicles_initial()
            self.__worker_thread = threading.Thread(target=self.__process_job_queue)
            self.__scheduler_logger.info("Starting initial scheduler run")
            self.__fetch_scheduler.run_all()
            self.__compress_scheduler.every(self.compress_interval).hours.do(
                self.__job_queue.put, self.__compress
            ).tag("compress")
            self.__scheduler_logger.info(
                f"Schedule S3 compressing at {self.compress_interval}"
            )
            self.__ingester_logger.info("Starting worker thread")
            self.__worker_thread.start()
            self.__scheduler_logger.info("Starting scheduler")
            while not self.__shutdown_requested.is_set():
                now = datetime.now().hour
                if now >= 6 and now <= 23:
                    self.__fetch_scheduler.run_pending()
                else:
                    self.__compress_scheduler.run_pending()
                time.sleep(1)
            self.__shutdown()

