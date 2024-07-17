import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

def flatten_multi_indexed_columns(df: DF) -> DF:
    df.columns = ['_'.join(col).strip() for col in df.columns.values]

    return df

def total_MB_memory_usage(df: DF) -> int:
    return df.memory_usage().sum() / 1e6
