from typing import TypeVar, Any
import logging

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

logger = logging.getLogger(__name__)

def flatten_multi_indexed_columns(self: DF) -> DF:
    self.columns = self.columns.map('_'.join).str.strip()

    return self

pd.DataFrame.flatten_multi_indexed_columns = flatten_multi_indexed_columns

PD_OBJ = TypeVar('T', pd.DataFrame, pd.Series)
def print_data(data: PD_OBJ) -> PD_OBJ:
    print(data)
    print(data.dtypes)
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

def str_split_and_retain_src(src: Series, pattern:str, n:int=None, col_names:list[str]=None,) -> DF:
    """
    ### Description:
    Splits the series according to a pattern and returns resulting df concatanated with the src series as the last column.
    """
    split_df = src.str.split(pattern, n=n, expand=True)
    result = pd.concat((src, split_df), axis="columns")
    if not col_names is None:
        result.columns = col_names

    return result

def uniques_as_series(s:Series) -> Series:
    """
    Warp around Series.unique to return another Series instead of an ndarray.
    """
    return Series(s.unique())

def concat(objects:list|dict|Series, **kwargs) -> DF:
    """
    Warp around pd.concat to work on Series and not emmit empty object.  
    Empty objects will be ignored.  
    If objects is a Series, the index will be ignores.  
    """
    if isinstance(objects, Series):
        return pd.concat([value for _, value in objects.items() if not value.empty], **kwargs)
    if isinstance(objects, list):
        return pd.concat([value for value in objects if not value.empty], **kwargs)
    if isinstance(objects, dict):
        return pd.concat({key:value for key, value in objects.items()}, **kwargs)
    
    raise ValueError(f"pandas_utils.concat recieved inappropriate type: {type(objects)}.\n{objects}")

def map_col_to_dict(df:DF, col:str, dict_map:dict) -> DF:
    df[col] = df[col].map(dict_map).fillna(df[col])
    
    return df

def set_str_to_lower(df: DF) -> DF:
    str_cols = df.select_dtypes(include='string').columns
    df.loc[:, str_cols] = df.loc[:, str_cols].apply(lambda col: col.str.lower())

    return df

def merge_with_columns(df_a:DF, df_b:DF, cols_to_copy:list, merge_on:str, **merge_kwargs) -> DF:
    """
    Merge `df_a` with a subset of `df_b` specified by `cols_to_copy` and `merge_on`.

    Parameters:
        df_a (pd.DataFrame): The left DataFrame to merge.
        df_b (pd.DataFrame): The right DataFrame to merge.
        cols_to_copy (list): Columns to copy from `df_b`.
        merge_on (list): Columns to merge on.
        **merge_kwargs: Additional keyword arguments for pd.merge.
    
    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    # Ensure merge_on columns are included in df_b
    selected_columns = list(set(cols_to_copy + [merge_on]))

    if (merge_on in df_b.index.names) and (merge_on in df_b.columns):
        df_b = df_b.reset_index(drop=True)
    return df_a.merge(df_b[selected_columns], on=merge_on, how="left")


def safe_astype(df:DF, col_dtypes:dict) -> DF:
    """
    Warp around pd.astype to ignore errors.
    Removes keys from col_dtypes that are not in df.columns.
    """
    col_dtypes = {col:dtype for col, dtype in col_dtypes.items() if col in df.columns}
    return df.astype(col_dtypes, errors="ignore")

