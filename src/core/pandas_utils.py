from typing import TypeVar
import logging

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

logger = logging.getLogger(__name__)

def flatten_multi_indexed_columns(self: DF) -> DF:
    self.columns = self.columns.map('_'.join).str.strip()

    return self

pd.DataFrame.flatten_multi_indexed_columns = flatten_multi_indexed_columns

T = TypeVar('T', pd.DataFrame, pd.Series)
def log_data_and_return_same_data(data: T) -> T:
    logger.info(data)
    return data

def total_MB_memory_usage(df: DF) -> int:
    return df.memory_usage().sum() / 1e6
