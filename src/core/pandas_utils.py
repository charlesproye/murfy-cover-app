from typing import TypeVar, Any
from logging import Logger
import logging

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series
import numpy as np

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

def left_merge(lhs: DF, rhs: DF, left_on: str|list[str], right_on: str|list[str], src_dest_cols: list|dict|None= None, logger:Logger=logger) -> DF:
    """
    Perform a left merge of two DataFrames based on specified key columns.

    This function merges two DataFrames, `lhs` (left-hand side) and `rhs` (right-hand side).  
    It allows for flexible selection of source and destination  
    columns from the right DataFrame to be merged into the left DataFrame.

    Parameters:
    - lhs (DF): The left DataFrame to merge into.
    - rhs (DF): The right DataFrame to merge from.
    - left_on (str | list[str]): Column(s) in `lhs` to join on.
    - right_on (str | list[str]): Column(s) in `rhs` to join on.
    - src_dest_cols (list | dict | None): Specifies which columns from `rhs` to merge into `lhs`.  
      If None, all columns from `rhs` not in `right_on` are used. If a list, those columns are used  
      as both source and destination. If a dict, keys are source columns and values are destination columns.  

    Returns:
    - DF: The merged DataFrame with columns from `rhs` added to `lhs` based on the specified keys.

    Raises:
    - ValueError: If `src_dest_cols` is not None, list, or dict, or if `rhs` contains duplicate keys.
    """
    logger.info(f"left_merge called.")
    # Turn right_on and left_on into lists if they are not already.
    left_on = [left_on] if not isinstance(left_on, list) else left_on
    right_on = [right_on] if not isinstance(right_on, list) else right_on
    assert len(left_on) == len(right_on), f"left_on and right_on must be the same length, left_on is {len(left_on)} and right_on is: {len(right_on)}"
    if src_dest_cols is None:
        src_cols = [col for col in rhs.columns if col not in right_on]
        dest_cols = [col for col in rhs.columns if col not in right_on]
    elif isinstance(src_dest_cols, list):
        src_cols = dest_cols = pd.Index(src_dest_cols).intersection(rhs.columns)
    elif isinstance(src_dest_cols, pd.Index):
        src_cols = dest_cols = src_dest_cols
    elif isinstance(src_dest_cols, dict):
        src_dest_cols = {key:value for key, value in src_dest_cols.items() if key in rhs.columns}
        src_cols = list(src_dest_cols.keys())
        dest_cols = list(src_dest_cols.values())
    else:
        raise ValueError(f"src_dest_cols must be None, list, or dict, received: {type(src_dest_cols)}")
    lhs_idx = pd.MultiIndex.from_frame(lhs[left_on])
    rhs_idx = pd.MultiIndex.from_frame(rhs[right_on])
    lhs_mask = lhs_idx.isin(rhs_idx)
    if not lhs_mask.any():
        logger.debug("lhs_mask is all False(no lhs[left_on] key is present in rhs[right_on]), returning lhs unchanged.")
        return lhs
    lhs_idx = lhs_idx.intersection(rhs_idx)
    #rhs_idx = rhs_idx.intersection(lhs_idx)
    # Ideally we would use a multi index even if left_on and right_on are single columns.
    # But there seems to be a bug in pandas: when you set the index of rhs with a single level multi index the set_index will default to a base index.
    # This in turn will break the left merge as we will try to index rhs(with a base index) with lhs_idx(a multi index) and no keys will match.
    if len(left_on) == 1:
        lhs_idx = pd.Index(lhs_idx.get_level_values(0), name=left_on[0])
        rhs_idx = pd.Index(rhs_idx.get_level_values(0), name=right_on[0])
    if rhs_idx.has_duplicates:
        raise ValueError(f"Cannot perform left merge as rhs_idx has duplicates! {rhs_idx[rhs_idx.duplicated()]}")
    logger.debug("Assigning values.")
    rhs = rhs.set_index(rhs_idx)
    rhs_data = rhs.loc[lhs_idx.values, src_cols]
    lhs.loc[lhs_mask, dest_cols] = rhs_data

    return lhs


def safe_astype(df:DF, col_dtypes:dict, logger:Logger=logger) -> DF:
    """
    Warp around pd.astype to ignore errors.
    Removes keys from col_dtypes that are not in df.columns.
    """
    logger.info(f"safe_astype called.")
    col_dtypes = {col:dtype for col, dtype in col_dtypes.items() if col in df.columns}
    datetime_cols = [col for col, dtype in col_dtypes.items() if "datetime" in dtype]
    col_dtypes = {col:dtype for col, dtype in col_dtypes.items() if not "datetime" in dtype}
    logger.debug(f"dtypes:\n{col_dtypes}")
    logger.debug(f"datetime_cols:{datetime_cols}")
    df = df.astype(col_dtypes, errors="ignore")
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], format='mixed')
    dtypes_dict = df[df.columns.intersection(col_dtypes.keys())].dtypes.to_dict()
    if dtypes_dict != col_dtypes:
        logger.warning("safe_astype did not succeed in changing all dtypes.")
        logger.warning(f"final col_dtypes:\n{dtypes_dict}")
    return df

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
        "nuniques": Series(nunique_dict).astype("int"),
        "uniques": Series(uniques_dict),
        "count": df.count(),
        "density": df.count().div(len(df)),
        "memory_usage_in_MB": df.memory_usage().div(1e6),
        },
        index=df.columns
    )

def safe_locate(df:DF, index_loc:pd.Index=None, col_loc:pd.Index=None, logger:Logger=logger) -> DF:
    logger.info(f"safe_locate called.")
    if not isinstance(index_loc, pd.Index) and index_loc is not None:
        index_loc = pd.Index(index_loc)
    if not isinstance(col_loc, pd.Index) and col_loc is not None:
        col_loc = pd.Index(col_loc)
    if index_loc is not None:
        index_loc = df.index.intersection(index_loc)
    if col_loc is not None:
        col_loc = df.columns.intersection(col_loc)
    return df.loc[(index_loc if index_loc is not None else df.index), (col_loc if col_loc is not None else df.columns)]

def dropna_cols(df:DF, logger:Logger=logger) -> DF:
    logger.info(f"dropna_cols called.")
    logger.debug(f"all nan cols:\n{df.notna().any(axis=0)}")
    return df.loc[:, df.notna().any(axis=0)]

