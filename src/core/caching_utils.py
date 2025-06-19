from typing import Callable, TypeVar, ParamSpec, List
from os.path import exists, dirname
from abc import ABC, abstractmethod
from functools import wraps
from os import makedirs
import inspect
import logging
import pandas as pd
from pandas import DataFrame as DF

from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.config import *
from pyspark.sql import SparkSession


R = TypeVar('R')
P = ParamSpec('P')

logger = logging.getLogger("caching_utils")

class CachedETL(DF, ABC):
    def __init__(self, path: str, on: str, force_update: bool = False, bucket: S3_Bucket = bucket, **kwargs):
        """
        Initialize a CachedETL with caching capabilities.
        The calculation of the result of the ETL must be implemented in the abstract `run` method.  
        Works similarly to the cache_results decorator.  
        Please take a look at the readme to see how `cache_results` (and therefore CachedEtl) works.

        Args:
        - path (str): Path for the cache file.
        - on (str): Either 's3' or 'local_storage', specifying the type of caching.
        - force_update (bool): If True, regenerate and cache the result even if it exists.
        - bucket_instance (S3_Bucket): S3 bucket instance, defaults to the global bucket.
        """
        assert on in ["s3", "local_storage"], "CachedETL's 'on' argument must be 's3' or 'local_storage'"
        assert path.endswith(".parquet"), "Path must end with '.parquet'"

        # Determine if we need to update the cache
        if force_update or (on == "s3" and not bucket.check_file_exists(path)) or (on == "local_storage" and not exists(path)):
            data = self.run()  # Call the abstract run method to generate data
            if on == "s3":
                bucket.save_df_as_parquet(data, path)
            elif on == "local_storage":
                data.to_parquet(path)
        else:
            if on == "s3":
                data = bucket.read_parquet_df(path, **kwargs)
            elif on == "local_storage":
                data = pd.read_parquet(path, **kwargs)

        super().__init__(data)

    @abstractmethod
    def run(self) -> DF:
        """Abstract method to be implemented by subclasses to generate the DataFrame."""
        pass


class CachedETLSpark(ABC):
    def __init__(self, path: str, on: str, force_update: bool = False, bucket: S3_Bucket = bucket, spark: SparkSession = None, **kwargs):
        """
        Initialize a CachedETL with caching capabilities.
        The calculation of the result of the ETL must be implemented in the abstract `run` method.  
        Works similarly to the cache_results decorator.  
        Please take a look at the readme to see how `cache_results` (and therefore CachedEtl) works.

        Args:
        - path (str): Path for the cache file.
        - on (str): Either 's3' or 'local_storage', specifying the type of caching.
        - force_update (bool): If True, regenerate and cache the result even if it exists.
        - bucket_instance (S3_Bucket): S3 bucket instance, defaults to the global bucket.
        """
        print(f"spark = {spark}")
        assert spark is not None
        self._spark = spark
        assert on in ["s3", "local_storage"], "CachedETL's 'on' argument must be 's3' or 'local_storage'"
        assert path.endswith(".parquet"), "Path must end with '.parquet'"

        # Determine if we need to update the cache
        if force_update or (on == "s3" and not bucket.check_spark_file_exists(path)) or (on == "local_storage" and not exists(path)):
            self.data = self.run()  # Call the abstract run method to generate data
            if on == "s3":
                bucket.save_df_as_parquet_spark(self.data, path)
            elif on == "local_storage":
                self.data.write.parquet(path)
        else:
            if on == "s3":
                self.data = bucket.read_parquet_df_spark(spark, path, **kwargs)
            elif on == "local_storage":
                self.data = spark.read.parquet(path, **kwargs)

    @abstractmethod
    def run(self) -> DF:
        """Abstract method to be implemented by subclasses to generate the DataFrame."""
        pass

def cache_result(path_template: str, on: str, path_params: List[str] = []):
    """
    Decorator to cache the results either locally or on S3 based on cache_type.  
    Please take a look at the core/readme to see examples of how to use this decorator.  

    Args:
    - path_template (str): Template path for the cache file.
    - cache_type (str): Either 's3' or 'local_storage', specifying the type of caching.
    - path_params (List[str]): List of argument names to be used for formatting the path.
    Function args:
    force_update (bool): Set to True to generate and cache the result even if it was already cached.  
    """
    assert on in ["s3", "local_storage"], "cache_type must be 's3' or 'local_storage'"
    def decorator(data_gen_func: Callable[..., pd.DataFrame]):
        @wraps(data_gen_func)
        def wrapper(*args, force_update=False, read_parquet_kwargs={}, **kwargs) -> pd.DataFrame:
            
            all_args = data_gen_func.__code__.co_varnames                                   # Extract the argument names and their values from args and kwargs 
            arg_values = {**dict(zip(all_args, args)), **kwargs}
            format_dict = {param: str(arg_values[param]) for param in path_params}
            path = path_template.format(**format_dict)                                      # Format the path using the specified parameters
            assert path.endswith(".parquet"), PATH_DOESNT_END_IN_PARQUET.format(path=path)  # Ensure the extension is ".parquet"
            if on == "s3":
                bucket, _ = get_bucket_from_func_args(data_gen_func, *args, **kwargs)       # Instantiate bucket if not provided
                if force_update or not bucket.check_file_exists(path):                      # Check if we need to update the cache or if the cache does not exist
                    data: pd.DataFrame = data_gen_func(*args, **kwargs)                     # Generate the data using the wrapped function
                    bucket.save_df_as_parquet(data, path)                                   # Save the data to S3 as parquet
                    return data
                else:
                    file = bucket.read_parquet_df(path, **read_parquet_kwargs)
                    return file            # Read cached data from S3
            elif on == "local_storage":
                if force_update or not exists(path):                                        # Check if we need to update the cache or if the cache does not exist
                    data: pd.DataFrame = data_gen_func(*args, **kwargs)                     # Generate the data using the wrapped function
                    save_cache_locally_to(data, path)                                       # Save the data locally
                    return data
                return pd.read_parquet(path, engine="pyarrow")                              # Read cached data from local file
        return wrapper
    return decorator


def cache_result_spark(path_template: str, on: str, path_params: List[str] = []):
    """
    Decorator to cache the results either locally or on S3 based on cache_type.  
    Please take a look at the core/readme to see examples of how to use this decorator.  

    Args:
    - path_template (str): Template path for the cache file.
    - cache_type (str): Either 's3' or 'local_storage', specifying the type of caching.
    - path_params (List[str]): List of argument names to be used for formatting the path.
    Function args:
    force_update (bool): Set to True to generate and cache the result even if it was already cached.  
    """
    assert on in ["s3", "local_storage"], "cache_type must be 's3' or 'local_storage'"
    def decorator(data_gen_func):
        @wraps(data_gen_func)
        def wrapper(*args, force_update=False, read_parquet_kwargs={}, **kwargs):
            
            all_args = data_gen_func.__code__.co_varnames
            arg_values = {**dict(zip(all_args, args)), **kwargs}
            format_dict = {param: str(arg_values[param]) for param in path_params}
            path = path_template.format(**format_dict)
            assert path.endswith(".parquet"), f"Cache path must end with .parquet, got: {path}"
            spark = arg_values.get("spark")
            assert isinstance(spark, SparkSession)
            if on == "s3":
                bucket, _ = get_bucket_from_func_args(data_gen_func, *args, **kwargs)
                s3_path = f"s3a://{bucket.bucket_name}/{path}"
                if force_update or not bucket.check_spark_file_exists(path):
                    data = data_gen_func(*args, **kwargs)
                    data.coalesce(1) \
                        .write \
                        .mode("append") \
                        .partitionBy("vin") \
                        .parquet(s3_path)
                    return data
                else:
                    return spark.read.parquet(s3_path, **read_parquet_kwargs)

            elif on == "local_storage":
                if force_update or not exists(path):
                    data = data_gen_func(*args, **kwargs)
                    data.write \
                        .partitionBy("vin") \
                        .option("parquet.block.size", 67108864) \
                        .mode("append") \
                        .parquet(s3_path)
                    return data
                return spark.read.parquet(path, **read_parquet_kwargs)

        return wrapper
    return decorator

def get_bucket_from_func_args(func:Callable, *args, **kwargs) -> tuple[S3_Bucket, bool]:
    signature = inspect.signature(func)                                                     # Get the function's signature
    bound_args = signature.bind_partial(*args, **kwargs)                                    # Map the positional args to the parameter names
    bound_args.apply_defaults()                                                             # Apply default values to the bound arguments
    bucket_is_in_func_args = 'bucket' in bound_args.arguments                               # Check if 'bucket' is in the arguments
    if not bucket_is_in_func_args:
        logger.debug(NO_BUCKET_ARG_FOUND.format(func_name=func.__name__))
    bucket_value = bound_args.arguments.get('bucket', bucket)
    # Return the bucket value and a bool indicating presence
    return bucket_value, bucket_is_in_func_args

def save_cache_locally_to(data: DF, path:str, **kwargs):
    """
    ### Description:
    Creates the parent dirs if they don't exist before saving the cache locally.
    """
    extension = path.split(".")[-1]
    if extension != "csv" and extension != "parquet":
        raise ValueError(f"Extension of path '{path}' is must be 'csv' or 'parquet'")
    ensure_that_local_dirs_exist(path)
    if extension == 'parquet':
        data.to_parquet(path, **kwargs)
    if extension == "csv":
        data.to_csv(path, **kwargs)

def ensure_that_local_dirs_exist(path:str):
    dir_path = dirname(path)
    if not exists(dir_path) and dir_path != "":
        makedirs(dir_path)

