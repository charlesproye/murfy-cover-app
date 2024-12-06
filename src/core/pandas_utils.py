from functools import reduce
from typing import TypeVar, Any
from logging import Logger
import logging

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series
from pandas import Index as Idx
from pandas import MultiIndex as MultiIdx
import numpy as np


PD_OBJ = TypeVar('T', pd.DataFrame, pd.Series)
logger = logging.getLogger(__name__)

def debug_df(df: DF, subset: list[str]=None, logger:Logger=logger) -> DF:
    show = print if logger is None else logger.debug
    df_to_debug = df if subset is None else df[subset]
    show(f"{df_to_debug}")
    show(sanity_check(df_to_debug))
    return df

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

def concat(objects:list|dict|Series, logger:Logger=logger, **kwargs) -> DF:
    """
    Warp around pd.concat to work on Series and not emmit empty object.  
    Empty objects will be ignored.  
    If objects is a Series, the index will be ignores.  
    """
    if len(objects) == 0:
        logger.debug("concat called with empty objects, returning empty DF.")
        return DF()
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

def set_all_str_cols_to_lower(df: DF, but:list[str]=[]) -> DF:
    str_cols = df.select_dtypes(include='string').columns.difference(but)
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
    if rhs.empty:
        logger.warning("rhs is empty, returning lhs unchanged.")
        return lhs
    # Turn right_on and left_on into lists if they are not already.
    left_on = [left_on] if not isinstance(left_on, list) else left_on
    right_on = [right_on] if not isinstance(right_on, list) else right_on
    assert len(left_on) == len(right_on), f"left_on and right_on must be the same length, left_on is {len(left_on)} and right_on is: {len(right_on)}"
    src_cols, dest_cols = src_dest_for_left_merge(lhs, rhs, left_on, right_on, src_dest_cols)
    logger.debug(f"src_cols: {src_cols}, dest_cols: {dest_cols}")
    lhs_keys = lhs[left_on].itertuples(index=False)
    rhs_keys = rhs[right_on].itertuples(index=False)
    lhs_idx = pd.MultiIndex.from_tuples(lhs_keys)
    rhs_idx = pd.MultiIndex.from_tuples(rhs_keys)
    lhs_mask = lhs_idx.isin(rhs_idx)
    if not lhs_mask.any():
        logger.debug("lhs_mask is all False(no lhs[left_on] key is present in rhs[right_on]), returning lhs unchanged.")
        return lhs
    lhs_idx = lhs_idx[lhs_mask]
    # Ideally we would use a multi index even if left_on and right_on are single columns.
    # But there seems to be a bug in pandas: when you set the index of rhs with a single level multi index the set_index will default to a base index.
    # This in turn will break the left merge as we will try to index rhs, with a base index, with lhs_idx, a multi index, and no keys will match.
    if len(left_on) == 1:
        lhs_idx = pd.Index(lhs_idx.get_level_values(0), name=left_on[0])
        rhs_idx = pd.Index(rhs_idx.get_level_values(0), name=right_on[0])
    if rhs_idx.has_duplicates:
        raise ValueError(f"Cannot perform left merge as rhs_idx has duplicates!\n{rhs_idx[rhs_idx.duplicated()]}")
    logger.debug("Assigning values.")
    rhs = rhs.set_index(rhs_idx)
    rhs_data = rhs.loc[lhs_idx.values, src_cols]
    lhs.loc[lhs_mask, dest_cols] = rhs_data.values

    return lhs

def src_dest_for_left_merge(lhs:DF, rhs:DF, left_on:list[str], right_on:list[str], src_dest_cols: list|dict|None) -> tuple[list, list]:
    if src_dest_cols is None:
        src_cols = [col for col in rhs.columns if col not in right_on]
        dest_cols = [col for col in rhs.columns if col not in right_on]
    elif isinstance(src_dest_cols, list):
        src_cols = dest_cols = pd.Index(src_dest_cols).intersection(rhs.columns).tolist()
    elif isinstance(src_dest_cols, pd.Index):
        src_cols = dest_cols = src_dest_cols
    elif isinstance(src_dest_cols, dict):
        src_dest_cols = {key:value for key, value in src_dest_cols.items() if key in rhs.columns}
        src_cols = list(src_dest_cols.keys())
        dest_cols = list(src_dest_cols.values())
    else:
        raise ValueError(f"src_dest_cols must be None, list, or dict, received: {type(src_dest_cols)}")
    return src_cols, dest_cols

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
    value_counts_dict = {}
    for col in df.columns:
        try:
            value_counts_dict[col] = df[col].value_counts().to_dict()
            nunique_dict[col] = float(df[col].nunique())
        except:
            value_counts_dict[col] = []
            nunique_dict[col] = np.nan
            
    return DF({
        "dtypes": df.dtypes.astype("string"),
        "nuniques": Series(nunique_dict).astype("float"),
        "value_counts": Series(value_counts_dict),
        "count": df.count(),
        "density": df.count().div(len(df)),
        "memory_usage_in_MB": df.memory_usage().div(1e6),
        "mean": df.mean(numeric_only=True),
        "std": df.std(numeric_only=True),
        "min": df.min(numeric_only=True),
        "max": df.max(numeric_only=True),
        "median": df.median(numeric_only=True),
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

    if col_loc is None and not index_loc is None:
        return df.loc[index_loc]
    if not col_loc is None and index_loc is None:
        return df.loc[:, col_loc]
    if not col_loc is None and not index_loc is None:
        return df.loc[index_loc, col_loc]
        
    raise ValueError("col_loc and index_loc cannot both be None.")

def dropna_cols(df:DF, logger:Logger=logger) -> DF:
    logger.info(f"dropna_cols called.")
    logger.debug(f"notna cols:\n{df.notna().any(axis=0)}")
    return df.loc[:, df.notna().any(axis=0)]

# WIP: this has only been tested with mobilisight data.
def parse_unstructured_json(json_obj, no_prefix_path:list[str]=[], no_suffix_path:list[str]=[], path:list[str]=[], logger:Logger=logger) -> DF:
    path = path.copy()
    if isinstance(json_obj, dict):
        df = DF(columns=["datetime"]).astype({"datetime": "datetime64[ns]"})
        for key, value in json_obj.items():
            item_df = parse_unstructured_json(value, no_prefix_path, no_suffix_path, path + [key])
            if "datetime" in item_df.columns:
                item_df["datetime"] = pd.to_datetime(item_df["datetime"], format="mixed").dt.tz_localize(None)
            if not item_df.empty:
                df = df.merge(item_df, how="outer", on="datetime")
        return df
    elif isinstance(json_obj, list):
        rename_cols_fct = lambda col: set_json_parsed_col_name(col, path, no_prefix_path, no_suffix_path)
        return pd.json_normalize(json_obj).rename(columns=rename_cols_fct)
    else:
        return DF()

def merge_dfs(dfs:list[DF]) -> DF:
    def join_two_dfs(df1:DF, df2:DF) -> DF:
        return pd.merge(df1, df2, how="outer", on="datetime")
    if len(dfs) > 0:
        return reduce(join_two_dfs, dfs)
    else:
        return DF()

def set_json_parsed_col_name(col:str, path:list[str], no_prefix_path:list[str], no_suffix_path:list[str]) -> str:
    if col in no_suffix_path and len(path) > 0:
        col = ""
    if not col in no_prefix_path:
        col = ".".join(path + [col] if col else path)

    return col

