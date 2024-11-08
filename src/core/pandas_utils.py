import inspect
from typing import TypeVar, Any
import logging

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.sql_utils import engine


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
    """Splits the series according to a pattern and returns resulting df concatanated with the src series as the first column."""
    split_df = src.str.split(pattern, n=n, expand=True)
    result = pd.concat((src, split_df), axis="columns")
    if not col_names is None:
        result.columns = col_names

    return result

def uniques_as_series(s:Series) -> Series:
    """Warp around Series.unique to return another Series instead of an ndarray."""
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

def set_all_str_cols_to_lower(df: DF) -> DF:
    str_cols = df.select_dtypes(include='string').columns
    df.loc[:, str_cols] = df.loc[:, str_cols].apply(lambda col: col.str.lower())

    return df

def left_merge(
    lhs: DF,
    rhs: DF,
    left_on: str | list[str],
    right_on: str | list[str],
    src_dest_cols: list | dict = None,
) -> DF:
    right_on = right_on if isinstance(right_on, list) else [right_on]
    left_on = left_on if isinstance(left_on, list) else [left_on]

    if src_dest_cols is None:
        src_cols = [col for col in rhs.columns if col not in right_on]
        dest_cols = [col for col in rhs.columns if col not in right_on]
    elif isinstance(src_dest_cols, list):
        src_cols = src_dest_cols
        dest_cols = src_dest_cols
    elif isinstance(src_dest_cols, dict):
        src_cols = list(src_dest_cols.keys())
        dest_cols = list(src_dest_cols.values())
    else:
        raise ValueError("src_dest_cols must be None, list, or dict")
    # Create masks for matching keys
    lhs_keys = lhs[left_on].apply(tuple, axis=1)
    rhs_keys = rhs[right_on].apply(tuple, axis=1)
    lhs_mask = lhs_keys.isin(rhs_keys)
    # Create a MultiIndex from the right_on columns
    rhs_index = pd.MultiIndex.from_tuples(rhs_keys.values, names=right_on)
    if rhs_index.has_duplicates:
        raise ValueError("rhs_index has duplicates!")
    lhs_index = pd.MultiIndex.from_tuples(lhs_keys[lhs_mask].values, names=left_on)
    # Merge the lhs and rhs DataFrames
    lhs.loc[lhs_mask, dest_cols] = rhs.set_index(rhs_index).loc[lhs_index.values, src_cols].values

    return lhs

def safe_astype(df:DF, col_dtypes:dict) -> DF:
    """
    Warp around pd.astype to ignore errors.
    Removes keys from col_dtypes that are not in df.columns.
    """
    col_dtypes = {col:dtype for col, dtype in col_dtypes.items() if col in df.columns}
    return df.astype(col_dtypes, errors="ignore")

def sanity_check(df:DF) -> DF:
    nunique_dict = {}
    uniques_dict = {}
    for col in df.columns:
        try:
            uniques_dict[col] = df[col].unique()
            nunique_dict[col] = float(df[col].nunique())
        except:
            uniques_dict[col] = []
            nunique_dict[col] = pd.NA
            
    return DF({
        "dtypes": df.dtypes.astype("string"),
        "nuniques": Series(nunique_dict),
        "uniques": Series(uniques_dict),
        "count": df.count(),
        "density": df.count().div(len(df)),
        "memory_usage_in_MB": df.memory_usage().div(1e6),
        },
        index=df.columns
    )

def safe_locate(df:DF, index_loc:pd.Index=None, col_loc:pd.Index=None) -> DF:
    if not isinstance(index_loc, pd.Index) and index_loc is not None:
        index_loc = pd.Index(index_loc)
    if not isinstance(col_loc, pd.Index) and col_loc is not None:
        col_loc = pd.Index(col_loc)
    if index_loc is not None:
        index_loc = df.index.intersection(index_loc)
    if col_loc is not None:
        col_loc = df.columns.intersection(col_loc)
    return df.loc[(index_loc if index_loc is not None else df.index), (col_loc if col_loc is not None else df.columns)]

def dropna_cols(df:DF) -> DF:
    return df.loc[:, df.notna().any(axis=0)]

