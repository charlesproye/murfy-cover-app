import concurrent.futures
import json
import logging
import os
import signal
import threading
from datetime import datetime
from types import FrameType

import dotenv
import msgspec
import schedule
from botocore.exceptions import ClientError

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from ingestion.high_mobility.api import HMApi
from ingestion.high_mobility.schema import all_brands
from ingestion.high_mobility.schema.brands import decode_vehicle_info
from ingestion.high_mobility.vehicle import Vehicle
from ingestion.ingestion_cache import IngestionCache


class HMIngester:
    __ingester_logger: logging.Logger
    __scheduler_logger: logging.Logger

    __api: HMApi
    __s3: S3Service
    __bucket: str

    __fetch_scheduler = schedule.Scheduler()
    __shutdown_requested = threading.Event()

    def __init__(
        self,
        refresh_interval: int = 2 * 60,
        max_workers: int = 4,
    ):
        """
        Parameters
        ----------
        refresh_interval: int, optional
            The interval at wich to update the vehicle list (in minutes)
            default: 120
        max_workers: int, optional
            The maximum numbers of workers (limited by the S3 bucket options)
            default: 4
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

        self.__ingester_logger = logging.getLogger("INGESTER")
        self.__scheduler_logger = logging.getLogger("SCHEDULER")

        self.__api = HMApi(HM_BASE_URL, HM_CLIENT_ID, HM_CLIENT_SECRET)

        # Use centralized S3 client
        s3_settings = S3Settings(
            S3_ENDPOINT=S3_ENDPOINT,
            S3_REGION=S3_REGION,
            S3_BUCKET=S3_BUCKET,
            S3_KEY=S3_KEY,
            S3_SECRET=S3_SECRET,
        )
        self.__s3 = S3Service(s3_settings)
        self.__bucket = S3_BUCKET
        self.__executor: concurrent.futures.ThreadPoolExecutor = (
            concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        )
        self.__ingestion_cache: IngestionCache = IngestionCache(
            make="highmobility", keys_to_ignore=["request_id", "*.timestamp"]
        )
        self.__vehicles: set[Vehicle] = set()

        self.refresh_interval: int = refresh_interval
        self.max_workers: int = max_workers

        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)

    def __handle_shutdown_signal(self, signum: int, _frame: FrameType | None):
        self.__ingester_logger.warn(
            f"Received signal {signal.Signals(signum).name}, shutting down"
        )
        self.__shutdown_requested.set()

    def __shutdown(self):
        self.__fetch_scheduler.clear()
        self.__ingester_logger.info("Canceled all jobs")
        self.__executor.shutdown(wait=True, cancel_futures=True)
        self.__ingester_logger.info("Cleared threadpool")
        self.__ingester_logger.info("Main thread stopped")

    def _fetch_clearances(self) -> list[Vehicle] | None:
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
        clearances = self._fetch_clearances()
        if clearances is None:
            return
        vehicles = {
            Vehicle(
                vin=clearance.vin,
                brand=clearance.brand,
                rate_limit=all_brands[clearance.brand].rate_limit,
                clearance_status=clearance.clearance_status,
            )
            for clearance in clearances
        }
        self.__ingester_logger.info(
            f"Fetched {len(vehicles)} with an approved clearance"
        )
        self.__vehicles.update(vehicles)
        for vehicle in vehicles:
            self.__fetch_scheduler.every(vehicle.rate_limit).seconds.do(
                self.__executor.submit,
                self._process_vehicle,
                vehicle,
            ).tag(vehicle.vin)
            self.__scheduler_logger.info(
                f"Adding vehicle with VIN {vehicle.vin} (brand {vehicle.brand}) to the scheduler (interval: {vehicle.rate_limit} seconds)"
            )
        self.__fetch_scheduler.every(self.refresh_interval).minutes.do(
            self.__update_vehicles
        ).tag("refresh")
        self.__scheduler_logger.info(
            f"Scheduled refresh of vehicle list in {self.refresh_interval} minutes"
        )

    def __update_vehicles(self) -> None:
        clearances = self._fetch_clearances()
        if clearances is None:
            return
        updated_vehicles = {
            Vehicle(
                vin=clearance.vin,
                brand=clearance.brand,
                rate_limit=all_brands[clearance.brand].rate_limit,
                clearance_status=clearance.clearance_status,
            )
            for clearance in clearances
        }
        vehicles_to_add = updated_vehicles.difference(self.__vehicles)
        vehicles_to_remove = self.__vehicles.difference(updated_vehicles)
        self.__vehicles.update(vehicles_to_add)
        self.__vehicles.difference_update(vehicles_to_remove)
        self.__ingester_logger.info(
            f"Updating VINs: {len(vehicles_to_remove)} to remove, {len(vehicles_to_add)} to add. "
            f"Currently {len(self.__fetch_scheduler.jobs)} scheduled jobs"
        )
        for v in vehicles_to_add:
            self.__scheduler_logger.info(
                f"Adding vehicle with VIN {v.vin} (brand {v.brand}) to scheduler (interval {v.rate_limit} seconds)"
            )
            self.__fetch_scheduler.every(v.rate_limit).seconds.do(
                self.__executor.submit,
                self._process_vehicle,
                v,
            ).tag(v.vin)
        for v in vehicles_to_remove:
            self.__scheduler_logger.info(
                f"Removing task for vehicle with VIN {v.vin} (brand {v.brand})"
            )
            self.__fetch_scheduler.clear(v.vin)

    def _parse_car_state_with_cache(
        self, vehicle: Vehicle, data: bytes
    ) -> bytes | None:
        json_data = json.loads(data)

        self.__ingester_logger.debug(
            f"Fetched vehicle info for vehicle with VIN {vehicle.vin} (brand {vehicle.brand})"
        )
        try:
            msgspec_data = decode_vehicle_info(data, vehicle.brand)
        except (msgspec.ValidationError, msgspec.DecodeError) as e:
            self.__ingester_logger.error(
                f"Unable to parse vehicle info for vehicle with VIN {vehicle.vin} (brand {vehicle.brand}): response {data} does not fit schema ({e})"
            )
            return

        self.__ingester_logger.debug(
            f"Parsed response for vehicle with VIN {vehicle.vin} correctly"
        )

        if self.__ingestion_cache.json_in_db(vin=vehicle.vin, json_data=json_data):
            self.__ingester_logger.debug(f"[{vehicle.vin}] Car state already in cache")
            return

        self.__ingestion_cache.set_json_in_db(vin=vehicle.vin, json_data=json_data)
        self.__ingester_logger.debug(f"[{vehicle.vin}] Successfully parsed data")

        try:
            return msgspec.json.encode(msgspec_data)
        except msgspec.EncodeError as e:
            self.__ingester_logger.error(f"Failed to encode vehicle data: {e}")
            return

    def _process_vehicle(
        self, vehicle: Vehicle, auto_upload: bool = True
    ) -> None | bytes:
        self.__ingester_logger.debug(
            f"Starting processing for vehicle with VIN {vehicle.vin} (brand {vehicle.brand})"
        )
        status_code, info = self.__api.get_vehicle_info(vehicle.vin)

        def log_error(info):
            self.__ingester_logger.error(
                f"Error getting info for VIN {vehicle.vin} (brand {vehicle.brand}) (request {info['request_id']}): {info['errors'][0]['title']} - {info['errors'][0]['detail']}"
            )

        match status_code:
            case 200:
                encoded = self._parse_car_state_with_cache(vehicle=vehicle, data=info)

                if (not auto_upload) or (not encoded):
                    return encoded

                filename = f"response/{vehicle.brand}/{vehicle.vin}/temp/{int(datetime.now().timestamp())}.json"

                try:
                    self.__s3.store_object(encoded, filename)
                    self.__ingester_logger.info(
                        f"Uploaded info for vehicle with VIN {vehicle.vin} at location {filename}"
                    )
                except ClientError as e:
                    self.__ingester_logger.error(
                        f"Error uploading info for vehicle with VIN {vehicle.vin}: {e.response['Error']['Message']}"
                    )
                return encoded
            case _:
                log_error(info)
                return

    def run(self):
        self.__schedule_tasks()
        self.__scheduler_logger.info("Starting scheduler")

        while not self.__shutdown_requested.is_set():
            idle_seconds = self.__fetch_scheduler.idle_seconds
            # Sleep for the minimum of idle time or 10 seconds, but at least 0.1 seconds
            sleep_time = max(
                0.1, min(idle_seconds if idle_seconds is not None else 10, 10)
            )

            if self.__shutdown_requested.wait(timeout=sleep_time):
                break

            self.__fetch_scheduler.run_pending()

        self.__shutdown()

    def __schedule_tasks(self):
        self.__update_vehicles_initial()
        self.__scheduler_logger.info(
            f"Starting initial scheduler run with {self.max_workers} workers "
            f"and {self.refresh_interval}min refresh interval"
        )
        self.__fetch_scheduler.run_all()
