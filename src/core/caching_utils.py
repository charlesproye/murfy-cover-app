from typing import Callable, TypeVar, ParamSpec, List
from os.path import exists, dirname
from os import makedirs
from functools import wraps
import inspect
import logging

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.config import *

R = TypeVar('R')
P = ParamSpec('P')

READ_FUNCTIONS: dict[str, Callable[..., DF]] = {
    "csv": pd.read_csv,
    "parquet": pd.read_parquet,
}

logger = logging.getLogger("caching_utils")

def cache_result(path_template: str, on: str, path_params: List[str] = []):
    """
    Decorator to cache the results either locally or on S3 based on cache_type.

    Args:
    - path_template (str): Template path for the cache file.
    - cache_type (str): Either 's3' or 'local_storage', specifying the type of caching.
    - path_params (List[str]): List of argument names to be used for formatting the path.
    Function args:
    force_update (bool): Set to True to generate and cache the result even if it was already cached.  
    """
    assert on in ["s3", "local_storage"], "cache_type must be 's3' or 'local'"
    def decorator(data_gen_func: Callable[..., pd.DataFrame]):
        @wraps(data_gen_func)
        def wrapper(*args, force_update=False, **kwargs) -> pd.DataFrame:
            # Extract the argument names and their values from args and kwargs
            all_args = data_gen_func.__code__.co_varnames
            arg_values = {**dict(zip(all_args, args)), **kwargs}
            # Format the path using the specified parameters
            path = path_template.format(**{param: str(arg_values[param]) for param in path_params})
            # Ensure the extension is ".parquet"
            assert path.endswith(".parquet"), PATH_DOESNT_END_IN_PARQUET.format(path=path)
            if on == "s3":
                # Instantiate bucket if not provided
                bucket, _ = get_bucket_from_func_args(data_gen_func, *args, **kwargs)

                # Check if we need to update the cache or if the cache does not exist
                if force_update or not bucket.check_file_exists(path):
                    # Generate the data using the wrapped function
                    data: pd.DataFrame = data_gen_func(*args, **kwargs)
                    # Save the data to S3 as parquet
                    bucket.save_df_as_parquet(data, path)
                    return data
                else:
                    # Read cached data from S3
                    return bucket.read_parquet_df(path)
            elif on == "local_storage":
                # Local cache handling
                if force_update or not exists(path):
                    # Generate the data using the wrapped function
                    data: pd.DataFrame = data_gen_func(*args, **kwargs)
                    # Save the data locally
                    save_cache_to(data, path)
                    return data
                # Read cached data from local file
                return pd.read_parquet(path)
        return wrapper
    return decorator

def get_bucket_from_func_args(func:Callable, *args, **kwargs) -> tuple[S3_Bucket, bool]:
    # Get the function's signature
    signature = inspect.signature(func)
    # Map the positional args to the parameter names
    bound_args = signature.bind_partial(*args, **kwargs)
    bound_args.apply_defaults()
    
    # Check if 'bucket' is in the arguments
    bucket_present = 'bucket' in bound_args.arguments
    if not bucket_present:
        logger.warning(NO_BUCKET_ARG_FOUND.format(func_name=func.__name__))
    bucket_value = bound_args.arguments.get('bucket', bucket)
    
    # Return the bucket value and a bool indicating presence
    return bucket_value, bucket_present

def save_cache_to(data: DF, path:str, **kwargs):
    """
    ### Description:
    Creates the parent dirs if they don't exist.
    """
    extension = path.split(".")[-1]
    if extension != "csv" and extension != "parquet":
        raise ValueError(f"Extension of path '{path}' is must be 'csv' or 'parquet'")
    ensure_that_dirs_exist(path)
    if extension == 'parquet':
        data.to_parquet(path, **kwargs)
    if extension == "csv":
        data.to_csv(path, **kwargs)

def ensure_that_dirs_exist(path:str):
    dir_path = dirname(path)
    if not exists(dir_path):
        makedirs(dir_path)

