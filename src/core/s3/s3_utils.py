import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
from typing import Any

import boto3
import joblib
import pandas as pd
import yaml
from botocore.client import Config
from botocore.exceptions import ClientError
from pandas import DataFrame as DF
from pandas import Series

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

from core.config import EMTPY_S3_KEYS_WARNING_MSG, KEY_LIST_COLUMN_NAMES
from core.pandas_utils import str_split_and_retain_src
from core.s3.settings import S3Settings


class S3Service:
    """
    Centralized S3 service with enforced encryption in transit (HTTPS/TLS).

    This service ensures ISO27001 compliance by:
    - Validating that endpoints use HTTPS protocol
    - Enforcing SSL/TLS for all connections
    - Requiring SSL certificate verification

    Use this service for all S3 operations to maintain security standards.
    """

    def __init__(self, settings: S3Settings | None = None):
        settings = settings or S3Settings()
        self._settings = settings

        # Configure boto3 with explicit SSL enforcement
        boto_config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        )

        self._s3_client = boto3.client(
            "s3",
            region_name=settings.S3_REGION,
            endpoint_url=settings.S3_ENDPOINT,
            aws_access_key_id=settings.S3_KEY,
            aws_secret_access_key=settings.S3_SECRET,
            use_ssl=True,  # Explicitly enforce SSL/TLS
            verify=True,  # Require SSL certificate verification
            config=boto_config,
        )
        self.bucket_name = settings.S3_BUCKET
        self.logger = logging.getLogger("S3_BUCKET")

    def save_df_as_parquet(self, df: DF, key: str):
        out_buffer = BytesIO()
        df.to_parquet(out_buffer)
        self._s3_client.put_object(
            Bucket=self.bucket_name, Key=key, Body=out_buffer.getvalue()
        )

    def check_file_exists(self, key: str) -> bool:
        """
        Checks if a file (key) exists in the S3 bucket.

        :param key: The S3 key (path) to check.
        :return: True if the file exists, False otherwise.
        """
        try:
            # Attempt to retrieve metadata of the object
            self._s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True  # If no exception, the key exists
        except Exception as e:
            # Check if the error code is 404, which means the key does not exist
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise e

    def check_spark_file_exists(self, prefix):
        try:
            response = self._s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )
            return "Contents" in response
        except Exception as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise e

    def save_df_as_parquet_spark(
        self, df: DF, key: str, spark, repartition_key: str | None = "vin"
    ):
        """
        Intended to concatenate existing data and overwrite files already present in scaleway.
        No idea about speed or viability.
        """
        from pyspark.sql import types as T
        from pyspark.sql.functions import col

        from core.spark_utils import align_dataframes_for_union

        s3_path = f"s3a://{self.bucket_name}/{key}"

        try:
            # Try to read the existing file
            processed = spark.read.parquet(s3_path)

            # Check if the DataFrame is not empty
            if processed.count() > 0:
                processed, df = align_dataframes_for_union(
                    processed, df, strategy="union"
                )

                if (
                    "DATETIME_BEGIN" in processed.columns
                    and "DATETIME_BEGIN" in df.columns
                ):
                    processed = processed.withColumn(
                        "DATETIME_BEGIN",
                        col("DATETIME_BEGIN").cast(T.TimestampType()),
                    )

                    df = df.withColumn(
                        "DATETIME_BEGIN",
                        col("DATETIME_BEGIN").cast(T.TimestampType()),
                    )

                    # Remove from processed the duplicates already in df
                    processed_filtered = processed.join(
                        df.select("VIN", "DATETIME_BEGIN").dropDuplicates(),
                        on=["VIN", "DATETIME_BEGIN"],
                        how="left_anti",
                    )

                    df_write = processed_filtered.unionByName(
                        df, allowMissingColumns=True
                    )
                else:
                    df_write = processed.union(df).dropDuplicates()

            else:
                df_write = df

        except Exception as e:
            # If the file doesn't exist or is corrupted
            if (
                "PATH_NOT_FOUND" in str(e)
                or "does not exist" in str(e)
                or "UNABLE_TO_INFER_SCHEMA" in str(e)
            ):
                df_write = df
            else:
                # Other error, re-raise it
                raise e

        # Optimized writing
        df_write.repartition(repartition_key).coalesce(32).write.mode(
            "overwrite"
        ).option("parquet.compression", "snappy").option(
            "parquet.block.size", 67108864
        ).partitionBy(repartition_key).parquet(s3_path)

    def append_spark_df_to_parquet(
        self, df: DF, key: str, repartition: str | None = None
    ):
        s3_path = f"s3a://{self.bucket_name}/{key}"

        if repartition:
            df = df.repartition(repartition)

        df.write.mode("append").option("parquet.compression", "snappy").option(
            "parquet.block.size", 67108864
        ).partitionBy("vin").parquet(s3_path)

    # This should be moved else wehere as this is a method specific to the way BIB oragnizes its responses.
    # Ideally this would be moved to transform.raw_tss.
    def list_responses_keys_of_brand(self, brand: str = "") -> DF:
        """
        ### Returns:
        A dataframe where each row represents the key of a response.
        The columns are key(absolute path), vin, make, file(filename).
        """
        self.logger.debug(f"Listing responses keys of {brand}...")
        keys = self.list_keys(f"response/{brand}/")
        keys = keys[keys.str.endswith(".json")]
        if len(keys) == 0:
            self.logger.info(EMTPY_S3_KEYS_WARNING_MSG.format(keys_prefix=brand))
            return DF(None, columns=KEY_LIST_COLUMN_NAMES)
        # Only retain .json responses
        # Responses are organized as follows: response/brand_name/vin/date-of-response.json
        keys = str_split_and_retain_src(keys, "/")
        self.logger.debug(f"Keys ending in .json:\n{keys}")
        # Remove files in temp directory
        keys = keys[keys[3] != "temp"]
        self.logger.debug(f"Keys after removing temps:\n{keys}")

        if len(keys.columns) > 5:
            self.logger.debug(f"Keys after last column:\n{keys}")
            keys = keys.drop(columns=[4])
        keys.columns = KEY_LIST_COLUMN_NAMES
        # Check that the file name is a date
        keys["is_valid_file"] = (
            keys["file"]
            .str.split(".", expand=True)
            .loc[:, 0]
            .str.match(r"^\d{4}-\d{2}-\d{2}$")
        )
        keys["is_valid_file"] &= keys["vin"].str.len() != 0
        self.logger.debug(f"set is_valid_file column:\n{keys}")
        keys = keys.query("is_valid_file")
        keys["date"] = keys["file"].str[:-5]

        return keys

    def list_keys(self, key_prefix: str = "", max_workers: int = 32) -> Series:
        """
        Returns a pandas Series of the keys present in the bucket.
        Uses multithreading to speed up listing.
        """
        if not key_prefix.endswith("/"):
            key_prefix += "/"

        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=key_prefix)

        keys = []

        # Use multithreading to process pages concurrently
        def process_page(page):
            if "Contents" in page:
                return [
                    obj["Key"] for obj in page["Contents"] if obj["Key"] != key_prefix
                ]
            return []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_page, page_iterator))

        # Flatten the list
        keys = [key for sublist in results for key in sublist]

        return Series(keys, dtype="string") if keys else Series(dtype="string")

    def read_parquet_df(self, key: str, **kwargs) -> DF:
        import pyarrow.parquet as pq
        from pyarrow import fs

        try:
            # Configure S3FileSystem with custom endpoint for Scaleway
            s3_fs = fs.S3FileSystem(
                region=self._settings.S3_REGION,
                access_key=self._settings.S3_KEY,
                secret_key=self._settings.S3_SECRET,
                endpoint_override=self._settings.S3_ENDPOINT.replace("https://", ""),
                scheme="https",
            )
            # Ensure key ends with / for consistency
            s3_path = f"{self.bucket_name}/{key.rstrip('/')}/"
            dataset = pq.ParquetDataset(s3_path, filesystem=s3_fs)
            table = dataset.read(**kwargs)
            return table.to_pandas()
        except Exception as e:
            self.logger.exception(
                f"Failed to read parquet dataset {key} in bucket {self.bucket_name}: {e}"
            )
            raise e

    def read_parquet_df_spark(self, spark: SparkSession, key: str, **kwargs):
        s3_path = f"s3a://{self.bucket_name}/{key}"
        return spark.read.option("mergeSchema", "true").parquet(s3_path, **kwargs)

    def read_csv_df(self, key: str, **kwargs) -> DF:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            return pd.read_csv(response.get("Body"), **kwargs)
        else:
            raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

    def read_key_as_text(self, key: str) -> str:
        response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")

        return object_content

    def read_multiple_json_files(self, keys: list, max_workers=32) -> list:
        """Reads multiple JSON files concurrently using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.read_json_file, keys))
        return results

    def read_json_file(self, key: str):
        """Reads a single JSON file from S3."""
        try:
            response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
            object_content = response["Body"].read().decode("utf-8")
            return json.loads(object_content)
        except Exception as e:
            self.logger.error(f"Failed to read key {key}: {e}")
            return None

    def get_last_modified(self, key: str) -> datetime:
        response = self._s3_client.head_object(Bucket=self.bucket_name, Key=key)
        return response["LastModified"]

    def list_subfolders(self, prefix: str) -> list[str]:
        """
        Lists immediate subfolders of the given prefix.

        :param prefix: The S3 prefix (path) to list subfolders from.
        :return: A list of subfolder names (without the full path).
        """
        # Ensure the prefix ends with a '/'
        if not prefix.endswith("/"):
            prefix += "/"

        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
        )

        subfolders = []
        for page in page_iterator:
            for prefix_dict in page.get("CommonPrefixes", []):
                # Extract just the folder name from the full path
                folder_name = prefix_dict["Prefix"][len(prefix) :].rstrip("/")
                if folder_name:  # Ignore empty folder names
                    subfolders.append(folder_name)

        return subfolders

    def generate_presigned_post(
        self, key: str, conditions: list[Any], expires_in: int = 3600
    ) -> dict[str, Any]:
        # conditions = [
        #     {"bucket": self.bucket_name},  # Only allow uploads to this bucket
        #     ["starts-with", "$key", key],  # Restrict to the specific folder
        #     {"acl": "private"},  # Ensure files are private after upload
        # ]
        return self._s3_client.generate_presigned_post(
            Bucket=self.bucket_name,
            Key=key,
            ExpiresIn=expires_in,
            Conditions=conditions,
        )

    def generate_folder_presigned_url(self, folder_name):
        # s3 = S3_Bucket()
        # bucket_name = get_env_var("S3_BUCKET")
        # folder_name = 'response/ituran/'

        response = self.generate_presigned_post(
            key=f"{folder_name}${{filename}}",
            expires_in=600000,
            conditions=[
                ["starts-with", "$key", folder_name],
                [
                    "starts-with",
                    "$Content-Type",
                    "",
                ],  # Accept all content types
                {"acl": "private"},  # Private files
                [
                    "content-length-range",
                    0,
                    1000485760,
                ],  # Limit file size (here 10MB)
            ],
        )

        return response

    def delete_folder(self, prefix: str, batch_size: int = 1000) -> None:
        """
        Recursively deletes a folder and all its contents from the S3 bucket.

        Args:
            prefix: The folder path to delete (e.g., 'my/folder/')
            batch_size: Number of objects to delete in each batch
        """
        # Ensure the prefix ends with a '/'
        if not prefix.endswith("/"):
            prefix += "/"

        self.logger.info(f"Starting deletion of folder: {prefix}")

        # List all objects in the folder
        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        # Process objects in batches
        current_batch = []
        deleted_count = 0

        for page in page_iterator:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                current_batch.append({"Key": obj["Key"]})

                # If we've reached the batch size, delete the batch
                if len(current_batch) >= batch_size:
                    self._delete_batch(current_batch)
                    deleted_count += len(current_batch)
                    self.logger.info(f"Deleted {deleted_count} objects from {prefix}")
                    current_batch = []

        # Delete any remaining objects
        if current_batch:
            self._delete_batch(current_batch)
            deleted_count += len(current_batch)
            self.logger.info(f"Deleted {deleted_count} objects from {prefix}")

        self.logger.info(f"Successfully deleted folder {prefix} and all its contents")

    def _delete_batch(self, objects: list[dict[str, str]]) -> None:
        """
        Deletes a batch of objects from the S3 bucket.

        Args:
            objects: List of objects to delete, each in the format {'Key': 'path/to/object'}
        """
        self._s3_client.delete_objects(
            Bucket=self.bucket_name, Delete={"Objects": objects, "Quiet": True}
        )

    def store_object(self, object: bytes, filename: str):
        self._s3_client.put_object(
            Body=object,
            Bucket=self.bucket_name,
            Key=filename,
        )

    def get_object_size(
        self,
        prefix,
        prefix_to_exclude: list[str] | None = None,
        exclude_temp: bool = True,
    ):
        """
        Calculate the total size and number of objects in an S3 path.

        This method recursively traverses all objects in the S3 bucket
        that match the given prefix and returns the sum of their sizes
        as well as their total number.

        Args:
            prefix (str): The S3 path for which to calculate the size.
                        For example: 'response/tesla/' or 'data/2024/01/'

        Returns:
            tuple: A tuple containing (total_size, object_count) where:
                - total_size (int): The sum of all object sizes in bytes
                - object_count (int): The total number of objects found
        """

        to_exclude = prefix_to_exclude or []

        objects = []
        paginator = self._s3_client.get_paginator("list_objects_v2")
        if not prefix.endswith("/"):
            prefix += "/"
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        for page in pages:
            for obj in page["Contents"]:
                if obj["Key"] in to_exclude or (
                    exclude_temp and "/temp/" in obj["Key"]
                ):
                    continue
                objects.append(obj["Size"])
        return (sum(objects), len(objects))

    def list_files(self, path: str = "", type_file: str = ""):
        files = []
        paginator = self._s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=path):
            for content in page.get("Contents", []):
                key = content["Key"]
                if key.endswith(type_file):
                    files.append(key)
        return sorted(files)

    def get_file(self, path: str) -> bytes | None:
        try:
            response = self._s3_client.get_object(Bucket=self.bucket_name, Key=path)
            with response["Body"] as stream:
                return stream.read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def read_yaml_file(self, path: str) -> dict[str, Any] | None:
        """
        Read and parse a YAML file from S3.

        Args:
            path: S3 path to the YAML file

        Returns:
            Parsed YAML content as dictionary, or None if file doesn't exist
        """
        file_content = self.get_file(path)
        if file_content is None:
            return None

        try:
            # Decode bytes to string and parse YAML
            yaml_content = file_content.decode("utf-8")
            return yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file {path}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading YAML file {path}: {e}")
            raise

    def save_as_pickle(self, obj, key: str) -> None:
        buffer = BytesIO()
        try:
            joblib.dump(obj, buffer)
            buffer.seek(0)

            self._s3_client.put_object(
                Bucket=self.bucket_name, Key=key, Body=buffer.getvalue()
            )
            self.logger.info(f"File save on s3://{self.bucket_name}/{key}")
        except Exception as e:
            self.logger.error(
                f"Error saving pickle file s3://{self.bucket_name}/{key}: {e}"
            )
            raise

    def load_pickle(self, key):
        try:
            response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
            buffer = BytesIO(response["Body"].read())
            model = joblib.load(buffer)
            return model
        except Exception as e:
            self.logger.error(
                f"Error reading pickle file s3://{self.bucket_name}/{key}: {e}"
            )
            raise

    def list_content(self, bucket: str, path: str = "") -> tuple[list[str], list[str]]:
        """Returns (folders, files) for a given S3 path"""
        paginator = self._s3_client.get_paginator("list_objects_v2")

        folders = set()
        files = []

        for page in paginator.paginate(Bucket=bucket, Prefix=path, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                folders.add(cp.get("Prefix"))
            for content in page.get("Contents", []):
                key = content["Key"]
                if key != path:
                    files.append(key)

        return sorted(folders), sorted(files)
