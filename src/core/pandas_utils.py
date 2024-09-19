from typing import TypeVar, Any
import logging

from rich import print
import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

logger = logging.getLogger(__name__)

def flatten_multi_indexed_columns(self: DF) -> DF:
    self.columns = self.columns.map('_'.join).str.strip()

    return self

pd.DataFrame.flatten_multi_indexed_columns = flatten_multi_indexed_columns

PD_OBJ = TypeVar('T', pd.DataFrame, pd.Series)
def log_data_and_return_same_data(data: PD_OBJ) -> PD_OBJ:
    logger.info(data)
    return data

def total_MB_memory_usage(df: DF) -> int:
    return df.memory_usage().sum() / 1e6

def floor_to(s:Series, quantization:float) -> Series:
    return (
        s
        .floordiv(quantization)
        .mul(quantization)
    )

def series_start_end_diff(s: Series) -> Any:
    return s.iat[-1] - s.iat[0]

def split_and_retain_src(src: Series, pattern:str, n:int=None, col_names:list[str]=None, ) -> DF:
    """
    ### Description:
    Splits the series according to a pattern and returns resulting df concatanated with the src series as the last column.
    """
    split_df = src.str.split(pattern, n=n, expand=True)
    result = pd.concat((split_df, src), axis="columns")
    if not col_names is None:
        result.columns = col_names

    return result
