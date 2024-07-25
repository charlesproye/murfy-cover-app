import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

def flatten_multi_indexed_columns(self: DF) -> DF:
    self.columns = ['_'.join(col).strip() for col in self.columns.values]
    return self

pd.DataFrame.flatten_multi_indexed_columns = flatten_multi_indexed_columns

# def min_max_weighting(self: DF, ) -> DF:

def total_MB_memory_usage(df: DF) -> int:
    return df.memory_usage().sum() / 1e6
