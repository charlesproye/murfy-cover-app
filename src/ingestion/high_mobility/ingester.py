import concurrent.futures
import logging
import os
import threading
import time
from datetime import datetime
from queue import Queue
from typing import Callable, Optional

import boto3
import dotenv
import msgspec
import schedule
from ingestion.high_mobility.api import HMApi
from ingestion.high_mobility.compress_data import HMCompresser
from ingestion.high_mobility.schema import (
    KiaInfo,
    MercedesBenzInfo,
    RenaultInfo,
)
from ingestion.high_mobility.vehicle import Vehicle


class HMIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: HMApi
    __s3 = boto3.client("s3")
    __bucket: str

    __scheduler = schedule.Scheduler()
    __vehicles: set[Vehicle] = set()
    __executor: concurrent.futures.ThreadPoolExecutor
    __job_queue: Queue[Callable]

    __json_encoder = msgspec.json.Encoder()
    __encode_buffer = bytearray()

    def __encode(self, obj):
        self.__json_encoder.encode_into(obj, self.__encode_buffer)
        return self.__encode_buffer

    rate_limit: dict[str, int] = {
        # Minimum time in seconds between two requests per vehicle for each maker
        "mercedes-benz": 36,
        "renault": 36,
        "kia": 24 * 60 * 60,
        "ford": 36,
    }

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
        self.__job_queue = Queue()
        self.refresh_interval = refresh_interval or self.refresh_interval
        self.compress_interval = compress_interval or self.compress_interval
        self.max_workers = max_workers or self.max_workers
        self.compress_threaded = compress_threaded or self.compress_threaded

        self.__ingester_logger = logging.getLogger("INGESTER")
        self.__scheduler_logger = logging.getLogger("SCHEDULER")

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
                rate_limit=self.rate_limit[clearance.brand],
                clearance_status=clearance.clearance_status,
            )
            for clearance in clearances
        )
        self.__ingester_logger.info(
            f"Fetched {len(vehicles)} with an approved clearance"
        )
        self.__vehicles.update(vehicles)
        for vehicle in vehicles:
            self.__scheduler.every(vehicle.rate_limit).seconds.do(
                self.__job_queue.put, lambda v=vehicle: self.__process_vehicle(v)
            ).tag(vehicle.vin)
            self.__scheduler_logger.info(
                f"Adding vehicle with VIN {vehicle.vin} (brand {vehicle.brand}) to the scheduler (interval: {vehicle.rate_limit} seconds)"
            )
        self.__scheduler.every(self.refresh_interval).minutes.do(
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
                    rate_limit=self.rate_limit[clearance.brand],
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
            self.__scheduler.every(v.rate_limit).seconds.do(
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
                    match vehicle.brand:
                        case "mercedes-benz":
                            decoded = msgspec.json.decode(info, type=MercedesBenzInfo)
                        case "renault":
                            decoded = msgspec.json.decode(info, type=RenaultInfo)
                        case "kia":
                            decoded = msgspec.json.decode(info, type=KiaInfo)
                        case _:
                            self.__ingester_logger.error(
                                f"Unable to parse vehicle info for vehicle with VIN {vehicle.vin} (brand {vehicle.brand}): unsupported vehicle brand"
                            )
                            return
                except msgspec.ValidationError as e:
                    self.__ingester_logger.error(
                        f"Unable to parse vehicle info for vehicle with VIN {vehicle.vin} (brand {vehicle.brand}): response {info} does not fit schema ({e})"
                    )
                    return
                self.__ingester_logger.info(
                    f"Parsed response for vehicle with VIN {vehicle.vin} correctly"
                )
                filename = f"response/{vehicle.brand}/{vehicle.vin}/temp/{int(datetime.now().timestamp())}.json"
                encoded = self.__encode(decoded)
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
                            f"Error uploading info for vehicle with vin {vehicle.vin}: {uploaded}"
                        )
                return
            case _:
                log_error(info)
                return

    def __compress(self):
        self.__ingester_logger.info("Starting compression job")
        compresser = HMCompresser(
            self.__s3,
            self.__bucket,
            threaded=self.compress_threaded,
            max_workers=self.max_workers,
        )
        compresser.run()

    def __process_job_queue(self):
        self.__ingester_logger.info("Starting processing job queue")
        while 1:
            job = self.__job_queue.get()
            self.__executor.submit(job)
            self.__job_queue.task_done()

    def run(self):
        if os.getenv("COMPRESS_ONLY"):
            self.__compress()
        else:
            self.__update_vehicles_initial()
            worker_thread = threading.Thread(target=self.__process_job_queue)
            self.__scheduler_logger.info("Starting initial scheduler run")
            self.__scheduler.run_all()
            self.__scheduler.every(self.compress_interval).hours.do(
                self.__job_queue.put, self.__compress
            ).tag("compress")
            self.__scheduler_logger.info(
                f"Schedule S3 compressing at {self.compress_interval}"
            )
            self.__ingester_logger.info("Starting worker thread")
            worker_thread.start()
            self.__scheduler_logger.info("Starting scheduler")
            while 1:
                self.__scheduler.run_pending()
                time.sleep(1)

