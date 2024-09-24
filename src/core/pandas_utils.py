from typing import TypeVar

from rich import print
import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

def flatten_multi_indexed_columns(self: DF) -> DF:
    self.columns = ['_'.join(col).strip() for col in self.columns.values]
    return self

pd.DataFrame.flatten_multi_indexed_columns = flatten_multi_indexed_columns

T = TypeVar('T', pd.DataFrame, pd.Series)
def print_data(data: T) -> T:
    print(data)
    return data

def total_MB_memory_usage(df: DF) -> int:
    return df.memory_usage().sum() / 1e6

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

def uniques_as_series(s:Series) -> Series:
    """
    Warp around Series.unique to return another Series instead of an ndarray.
    """
    return Series(s.unique())
