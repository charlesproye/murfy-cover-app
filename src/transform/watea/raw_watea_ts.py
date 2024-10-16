from typing import Generator
import os

import pandas as pd
from pandas import DataFrame as DF

from transform.watea.watea_constants import *

def raw_ts_it() -> Generator[tuple[str, DF], None, None]:
    for file in os.listdir("data_cache/raw_time_series"):
        if file.endswith(".parquet"):
            yield file[:6], pd.read_parquet("data_cache/raw_time_series/" + file)

def raw_ts_of(id: str) -> DF:
    return pd.read_parquet(PATH_TO_RAW_TS.format(id=id))
