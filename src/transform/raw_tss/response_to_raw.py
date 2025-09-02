import random
import re
import time
from datetime import datetime
from itertools import islice
from logging import Logger
from typing import Optional
import os
from dotenv import load_dotenv
import math

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.spark_utils import get_optimal_nb_partitions
from transform.raw_tss.config import S3_RAW_TSS_KEY_FORMAT, PARSE_TYPE_MAP

load_dotenv() 

class ResponseToRawTss:
    """
    Class to process data returned by APIs stored in /response on Scaleway
    """

    def __init__(
        self,
        make: str,
        writing_mode: Optional[str] = "append",
        spark: SparkSession = None,
        logger: Logger = None,
        **kwargs,
    ):
        """
        Initialize the telemetry processor

        Args:
            make: Vehicle brand
            bucket: S3Service instance for data access
            spark: Spark session for data processing
            force_update: If True, forces data update in CachedETLSpark
        """

        self.logger = logger
        self.spark = spark
        self.make = make
        self.bucket = S3Service()
        self.settings = S3Settings()
        self.base_s3_path = self.settings.S3_BASE_PATH
        self.raw_tss_path = S3_RAW_TSS_KEY_FORMAT.format(brand=self.make)

    def run(self):

        self.logger.info(f"Traitement débuté pour {self.make}")

        keys_to_download_per_vin, paths_to_exclude = (
            self._get_keys_to_download()
        ) 


        if len(keys_to_download_per_vin) == 0:
            self.logger.info(f"No VIN to process for {self.make}")
        else:
            optimal_partitions_nb, batch_size = self._set_optimal_spark_parameters(
                keys_to_download_per_vin, paths_to_exclude, int(os.environ.get("NB_CORES_CLUSTER"))
            )

            print(f"Nombre de batchs = {math.ceil(len(keys_to_download_per_vin) / batch_size)}")

            for batch_num, batch in enumerate(
                self._batch_dict_items(keys_to_download_per_vin, batch_size), 1
            ): 
                self.logger.info(f"Batch {batch_num}:")

                # Extract
                raw_tss_unparsed = self._download_keys(batch)
                
                # Transform
                raw_tss_parsed = self.parse_data(
                    raw_tss_unparsed, optimal_partitions_nb
                )

                # Load
                self.bucket.append_spark_df_to_parquet(
                    raw_tss_parsed, self.raw_tss_path
                )

                self._update_last_parsed_date(batch)
                end = time.time()

                raw_tss_parsed.unpersist()
                del raw_tss_parsed

        self.logger.info(f"Processing completed for {self.make}")

    def _set_optimal_spark_parameters(
        self,
        keys_to_download_per_vin: dict,
        paths_to_exclude: list[str],
        nb_cores: int = 8,
    ) -> tuple[int, int]:
        """
        Calculates the optimal batch size for parallel processing of VINs.

        This method determines the optimal number of VINs to process per batch based on
        data size, number of VINs, and available system resources.
        The goal is to balance the workload across CPU cores while
        maximizing Spark resource utilization.

        Args:
            nb_cores (int, optional): Number of CPU cores available for processing.
                                      Default: 4

        Returns:
            int: Optimal number of VINs to process per batch
        """
        if nb_cores <= 0:
            raise ValueError("Number of cores must be a positive integer")

        file_size, _ = self.bucket.get_object_size(
            f"response/{self.make}/", prefix_to_exclude=paths_to_exclude
        )

        nb_vins = len(list(keys_to_download_per_vin.keys()))

        if nb_vins == 0:
            self.logger.warning("No VINs to process, returning batch_size = 1")
            return 1

        optimal_partitions = get_optimal_nb_partitions(file_size, nb_vins)

        vin_per_batch = max(1, int((nb_vins / optimal_partitions) * nb_cores * 4))

        return (4 * nb_cores, vin_per_batch)

    def _group_paths_by_vin(self, paths: list[str]) -> dict[str, list[str]]:
        grouped = {}

        for path in paths:
            if "/temp/" not in path:
                parts = path.strip("/").split("/")
                if len(parts) < 2:
                    continue  # skip invalid paths
                vin = parts[-2]
                # Initialize list if vin not seen yet
                if vin not in grouped:
                    grouped[vin] = []

                grouped[vin].append(path)

        return grouped

    def _batch_dict_items(self, dictionary: dict, batch_size: int):
        """Generator to process a dictionary in batches"""
        total_items = len(dictionary)

        for i in range(0, total_items, batch_size):
            batch = dict(islice(dictionary.items(), i, i + batch_size))
            yield batch

    def _get_keys_to_download(self) -> (dict[str, list[str]], list):
        """
        Retrieves the S3 keys of the files to download by filtering based on the last analysis date.

        This method compares the dates of the available response files with the last
        analysis date stored in the raw TSS data to determine which files
        need to be downloaded and processed.

        Returns:
            dict[str, list[str]]: Dictionary where keys are VINs and values are
                                  lists of S3 paths of files to download.
                                  Format: {'VIN123': ['response/brand/VIN123/2024-01-01.json', ...]}

        Raises:
            Exception: If an error occurs while reading the Parquet data or
                       listing S3 files
        """

        last_parsed_date_dict = None

        if self.bucket.check_spark_file_exists(
            f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet"
        ):
            last_parsed_date_df = self.bucket.read_parquet_df_spark(
                self.spark,
                f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet",
            )
            last_parsed_date_dict = dict(
                last_parsed_date_df.select("vin", "last_parsed_file_date").collect()
            )

        vins_paths = self.bucket.list_files(f"response/{self.make}/", type_file=".json")

        vins_paths_grouped = self._group_paths_by_vin(vins_paths)


        paths_to_exclude = []

        if last_parsed_date_dict:
            for vin, paths in vins_paths_grouped.items():
                if vin in last_parsed_date_dict.keys():
                    vins_paths_grouped[vin] = [
                        path
                        for path in paths
                        if datetime.strptime(path.split("/")[-1], "%Y-%m-%d.json")
                        > datetime.strptime(
                            str(last_parsed_date_dict[vin]).split()[0], "%Y-%m-%d"
                        )
                    ]


                    paths_to_exclude.extend(
                        [
                            path
                            for path in paths
                            if datetime.strptime(path.split("/")[-1], "%Y-%m-%d.json")
                            <= datetime.strptime(
                                str(last_parsed_date_dict[vin]).split()[0], "%Y-%m-%d"
                            )
                        ]
                    )


        vins_paths_grouped = {k: v for k, v in vins_paths_grouped.items() if v}

        vins_paths_grouped = {k: v for k, v in vins_paths_grouped.items() if len(v) > 0}

        # Shuffle the vins to avoid skewness
        vins_paths_grouped = dict(
            random.sample(list(vins_paths_grouped.items()), k=len(vins_paths_grouped))
        )

        return (vins_paths_grouped, paths_to_exclude)

    def _download_keys(self, batch: dict[str, list[str]]) -> DataFrame:
        """
        Downloads JSON files and returns a Spark DataFrame
        """

        keys_to_download = []

        for _, paths in batch.items():
            keys_to_download.extend(paths)


        keys_to_download_str = [
            f"{self.settings.S3_BASE_PATH}/{key}" for key in keys_to_download
            ]


        field_def = self.bucket.read_yaml_file(f"config/{self.make}.yaml")

        if self.make != 'tesla-fleet-telemetry':
            field_def = field_def['response_to_raw']

        schema = self._get_dynamic_schema(field_def, PARSE_TYPE_MAP)

        return (
            self.spark.read.option("multiline", "true")
            .option(
                "badRecordsPath",
                f"{self.settings.S3_BASE_PATH}/response/{self.make}/corrupted_responses/",
            )
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .json(keys_to_download_str)
        )

    def _update_last_parsed_date(self, keys_to_download_per_vin: dict):
        """
        Updates the last parsed date for the VINs present in the DataFrame.
        """

        def extract_date_from_path(path):
            match = re.search(r"(\d{4}-\d{2}-\d{2})\.json$", path)
            return match.group(1) if match else None

        rows = []
        for vin, paths in keys_to_download_per_vin.items():
            dates = [
                extract_date_from_path(p) for p in paths if extract_date_from_path(p)
            ]
            if dates:
                dates = [date for date in dates if date]  # Remove None
                max_date_str = max(dates)
                rows.append(Row(vin=vin, last_parsed_file_date=max_date_str))

        schema = StructType(
            [
                StructField("vin", StringType(), False),
                StructField("last_parsed_file_date", StringType(), False),
            ]
        )

        progress_df = self.spark.createDataFrame(rows, schema=schema)

        vins_to_update = list(keys_to_download_per_vin.keys())

        if self.bucket.check_spark_file_exists(
            f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet"
        ):
            existing_df = self.bucket.read_parquet_df_spark(
                self.spark,
                f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet",
            )
            filtered_df = existing_df.filter(~existing_df.vin.isin(vins_to_update))
            if filtered_df.count() > 0:
                final_df = filtered_df.unionByName(progress_df)
                final_df.cache().count()
            else:
                final_df = progress_df
        else:
            final_df = progress_df

        final_df.coalesce(1).write.mode("overwrite").parquet(
            f"{self.settings.S3_BASE_PATH}/raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet"
        )

        pass

    def parse_data(self, df: DataFrame, optimal_partitions_nb: int) -> DataFrame:
        return df

    def _get_dynamic_schema(self, field_def: dict, parse_type_map: dict) -> DataFrame:
        return StructType()
