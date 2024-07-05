"""
This module provides the function `raw_time_series_of` to provide a raw time series from Tesla's personal API.
Can also be used as a script to obtain the entirety of the tesla fleet data.
"""
import pandas as pd
from pandas import DataFrame as DF

from watea_constants import *

def raw_ts_of(id: str) -> DF:
    return pd.read_parquet(PATH_TO_RAW_TS.format(id=id))
