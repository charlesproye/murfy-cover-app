import logging
from abc import ABC, abstractmethod
from os import makedirs
from os.path import dirname, exists
from typing import ParamSpec, TypeVar

from pandas import DataFrame as DF
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from core.spark_utils import create_spark_session

from .s3.s3_utils import S3Service
from .s3.settings import S3Settings

R = TypeVar("R")
P = ParamSpec("P")

logger = logging.getLogger("caching_utils")


class CachedETLSpark(ABC):
    def __init__(
        self,
        on,
        bucket: S3Service,
        settings: S3Settings,
        spark: SparkSession | None = None,
        writing_mode: str | None = None,
        repartition_key: str | None = "vin",
        **kwargs,
    ):
        """
        Initialize a CachedETL with caching capabilities.
        The calculation of the result of the ETL must be implemented in the abstract `run` method.
        Works similarly to the cache_results decorator.
        Please take a look at the readme to see how `cache_results` (and therefore CachedEtl) works.

        Args:
        - path (str): Path for the cache file.
        - on (str): Either 's3' or 'local_storage', specifying the type of caching.
        - force_update (bool): If True, regenerate and cache the result even if it exists.
        - bucket_instance (S3Service): S3 bucket instance, defaults to the global bucket.
        """

        if spark is None:
            spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)
        self.spark: SparkSession = spark
        assert on in [
            "s3",
            "local_storage",
        ], "CachedETL's 'on' argument must be 's3' or 'local_storage'"

        self.on = on
        self.bucket = bucket
        self.settings = settings
        self.writing_mode = writing_mode
        self.repartition_key = repartition_key
        self.kwargs = kwargs

    @abstractmethod
    def run(self) -> DF:
        """Abstract method to be implemented by subclasses to generate the DataFrame."""

    def save(self, data: SparkDF, path: str, force_update: bool = False) -> SparkDF:
        # Determine if we need to update the cache
        assert path.endswith(".parquet"), "Path must end with '.parquet'"

        if (
            force_update
            or (self.on == "s3" and not self.bucket.check_spark_file_exists(path))
            or (self.on == "local_storage" and not exists(path))
        ):
            if self.on == "s3":
                if self.writing_mode == "append":
                    self.bucket.append_spark_df_to_parquet(data, path)
                else:
                    self.bucket.save_df_as_parquet_spark(
                        data, path, self.spark, self.repartition_key
                    )
            elif self.on == "local_storage":
                data.write.mode("overwrite").parquet(path)
        else:
            if self.on == "s3":
                data = self.bucket.read_parquet_df_spark(
                    self.spark, path, **self.kwargs
                )
            elif self.on == "local_storage":
                data = self.spark.read.parquet(path, **self.kwargs)
        return data


def save_cache_locally_to(data: DF, path: str, **kwargs):
    """
    ### Description:
    Creates the parent dirs if they don't exist before saving the cache locally.
    """
    extension = path.split(".")[-1]
    if extension != "csv" and extension != "parquet":
        raise ValueError(f"Extension of path '{path}' is must be 'csv' or 'parquet'")
    ensure_that_local_dirs_exist(path)
    if extension == "parquet":
        data.to_parquet(path, **kwargs)
    if extension == "csv":
        data.to_csv(path, **kwargs)


def ensure_that_local_dirs_exist(path: str):
    dir_path = dirname(path)
    if not exists(dir_path) and dir_path != "":
        makedirs(dir_path)
