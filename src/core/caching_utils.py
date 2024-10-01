from typing import Callable, TypeVar, ParamSpec, Generator, Union
from os.path import exists, dirname
from os import makedirs
from glob import glob
from functools import wraps

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

R = TypeVar('R')
P = ParamSpec('P')

READ_FUNCTIONS: dict[str, Callable[..., DF]] = {
    "csv": pd.read_csv,
    "parquet": pd.read_parquet,
}

def instance_data_caching_wrapper(vin: str, path_to_cache: str, data_gen_func: Callable[P, R], force_update=False, read_kwargs={}, write_kwargs={},  **kwargs: P.kwargs) -> R:
    """
    ### Description:
    Utilitary function to abstract away caching implementation of dataframe.
    """
    path_to_cache = path_to_cache.format(vin=vin)
    extension = path_to_cache.split(".")[-1]
    if extension != "csv" and extension != "parquet":
        raise ValueError(f"Extension of path '{path_to_cache}' is must be 'csv' or 'parquet'")
    if force_update or not exists(path_to_cache):
        data: DF|Series = data_gen_func(vin, **kwargs)
        save_cache_to(data, path_to_cache, **write_kwargs)
        return data
    
    return READ_FUNCTIONS[extension](path_to_cache, **read_kwargs)

def singleton_data_caching(path_to_cache: str):
    def decorator(data_gen_func: Callable[P, R]):
        @wraps(data_gen_func)
        def wrapper(*args: P.args, force_update=False, **kwargs: P.kwargs) -> R:
            extension = path_to_cache.split(".")[-1]
            if extension != "csv" and extension != "parquet":
                raise ValueError(f"Extension of path '{path_to_cache}' must be 'csv' or 'parquet'")
            
            if force_update or not exists(path_to_cache):
                data: Union[DF, Series] = data_gen_func(*args, **kwargs)
                save_cache_to(data, path_to_cache)
                return data
            
            return READ_FUNCTIONS[extension](path_to_cache)
        
        return wrapper
    
    return decorator

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

