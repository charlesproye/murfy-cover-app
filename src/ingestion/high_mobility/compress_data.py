import logging
from datetime import datetime

import boto3
import msgspec
from ingestion.high_mobility.schema import (
    KiaInfo,
    MercedesBenzInfo,
    MergedKiaInfo,
    MergedMercedesBenzInfo,
    MergedRenaultInfo,
    RenaultInfo,
)


class HMCompresser:
    __logger: logging.Logger

    __s3 = boto3.client("s3")

    __kia: dict[str, set[str]]
    __renault: dict[str, set[str]]
    __mercedes_benz: dict[str, set[str]]

    threaded: bool = False
    max_workers: Optional[int] = 8

    def __init__(
        self, s3, bucket, threaded: bool = False, max_workers: Optional[int] = 8
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__s3 = s3
        self.__bucket = bucket
        self.__logger.info("Listing bucket objects")
        self.list_objects()
        self.threaded = threaded

    def list_objects(self):
        paginator = self.__s3.get_paginator("list_objects_v2")
        it = paginator.paginate(Bucket=self.__bucket, Prefix="response")
        kia = set()
        renault = set()
        mercedes_benz = set()
        for obj in it:
            for con in obj["Contents"]:
                key = con["Key"]
                match key.split("/")[1]:
                    case "kia":
                        kia.add(key)
                    case "mercedes-benz":
                        mercedes_benz.add(key)
                    case "renault":
                        renault.add(key)
        kia_vins = set(
            filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], kia))
        )
        self.__kia = {
            k: set(filter(lambda e: e.startswith(f"response/kia/{k}/temp/"), kia))
            for k in kia_vins
        }
        self.__logger.info("Listed all kia vehicles")
        renault_vins = set(
            filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], renault))
        )
        self.__renault = {
            k: set(
                filter(lambda e: e.startswith(f"response/renault/{k}/temp/"), renault)
            )
            for k in renault_vins
        }
        self.__logger.info("Listed all renault vehicles")
        mercedes_benz_vins = set(
            filter(
                lambda v: len(v) == 17, map(lambda v: v.split("/")[2], mercedes_benz)
            )
        )
        self.__mercedes_benz = {
            k: set(
                filter(
                    lambda e: e.startswith(f"response/mercedes-benz/{k}/temp/"),
                    mercedes_benz,
                )
            )
            for k in mercedes_benz_vins
        }
        self.__logger.info("Listed all mercedes-benz vehicles")

    def __process(self, items, brand):
        def merge(temp_data: set[str]):
            merged = None
            for s3_key in temp_data:
                get_response = self.__s3.get_object(Bucket=self.__bucket, Key=s3_key)
                match get_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 200:
                        self.__logger.info(
                            f"Fetched temporary data {s3_key} (brand {brand}) successfully"
                        )
                        content = get_response["Body"].read().decode("utf-8")
                    case _:
                        self.__logger.error(
                            f"Failed to fetch temporary data {s3_key} (brand {brand}): {get_response}"
                        )
                        return None
                match brand:
                    case "kia":
                        try:
                            parsed = msgspec.json.decode(content, type=KiaInfo)
                        except msgspec.ValidationError as e:
                            self.__logger.error(
                                f"Failed to parse temporary data {s3_key} (brand {brand}): {e}"
                            )
                            return None
                        if parsed is not None:
                            if merged is None:
                                merged = MergedKiaInfo.from_initial(parsed)
                            else:
                                if isinstance(merged, MergedKiaInfo):
                                    merged.merge(parsed)
                                else:
                                    self.__logger.error(
                                        "Cannot compress different vehicle types together"
                                    )
                                    return
                    case "renault":
                        try:
                            parsed = msgspec.json.decode(content, type=RenaultInfo)
                        except msgspec.ValidationError as e:
                            self.__logger.error(
                                f"Failed to parse temporary data {s3_key} (brand {brand}): {e}"
                            )
                            return None
                        if parsed is not None:
                            if merged is None:
                                merged = MergedRenaultInfo.from_initial(parsed)
                            else:
                                if isinstance(merged, MergedRenaultInfo):
                                    merged.merge(parsed)
                                else:
                                    self.__logger.error(
                                        "Cannot compress different vehicle types together"
                                    )
                                    return
                    case "mercedes-benz":
                        try:
                            parsed = msgspec.json.decode(content, type=MercedesBenzInfo)
                        except msgspec.ValidationError as e:
                            self.__logger.error(
                                f"Failed to parse temporary data {s3_key} (brand {brand}): {e}"
                            )
                            return None
                        if parsed is not None:
                            if merged is None:
                                merged = MergedMercedesBenzInfo.from_initial(parsed)
                            else:
                                if isinstance(merged, MergedMercedesBenzInfo):
                                    merged.merge(parsed)
                                else:
                                    self.__logger.error(
                                        "Cannot compress different vehicle types together"
                                    )
                                    return
                delete_response = self.__s3.delete_object(
                    Bucket=self.__bucket, Key=s3_key
                )
                match delete_response["ResponseMetadata"]["HTTPStatusCode"]:
                    case 204:
                        self.__logger.info(
                            f"Deleted temporary datapoint {s3_key} (brand {brand})"
                        )
                    case _:
                        self.__logger.error(
                            f"Error deleting temporary datapoint {s3_key} (brand {brand}): {delete_response}"
                        )
                        return None
            return merged

        for vin, temp_data in items.items():
            today = datetime.today().date()
            self.__logger.info(f"Compressing data for VIN {vin} (brand {brand})")
            try:
                encoded = msgspec.json.encode(merge(temp_data))
            except msgspec.EncodeError as e:
                self.__logger.error(
                    f"Failed to encode merged vehicle data for VIN {vin} (brand {brand}): {e}"
                )
                return None
            put_response = self.__s3.put_object(
                Bucket=self.__bucket,
                Key=f"response/{brand}/{vin}/{today}.json",
                Body=encoded,
            )
            match put_response["ResponseMetadata"]["HTTPStatusCode"]:
                case 200:
                    self.__logger.info(
                        f"Uploaded compressed data for VIN {vin} (brand {brand}) at location response/{brand}/{vin}/{today}.json"
                    )
                case _:
                    self.__logger.error(
                        f"Failed to upload compressed data for VIN {vin} (brand {brand}): {put_response}"
                    )

    def compress_kia(self):
        if not self.threaded:
            self.__process(self.__kia, "kia")

    def compress_renault(self):
        if not self.threaded:
            self.__process(self.__renault, "renault")

    def compress_mercedes_benz(self):
        if not self.threaded:
            self.__process(self.__mercedes_benz, "mercedes-benz")

    def run(self):
        self.__logger.info("Starting kia data compression")
        self.compress_kia()
        self.__logger.info("Starting mercedes-benz data compression")
        self.compress_mercedes_benz()
        self.__logger.info("Starting renault data compression")
        self.compress_renault()

