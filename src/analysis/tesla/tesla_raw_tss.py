from glob import glob
import logging

import pandas as pd
from pandas import DataFrame as DF

from core.pandas_utils import total_MB_memory_usage
from core.console_utils import main_decorator
from core.caching_utils import singleton_data_caching
from analysis.tesla.tesla_constants import *

logger = logging.getLogger(__name__)

@main_decorator
def main():
    raw_tss = get_raw_tss(force_update=True)
    print(raw_tss)
    print("total MB memory usage:", total_MB_memory_usage(raw_tss))

@singleton_data_caching(RAW_TSS_PATH)
def get_raw_tss() -> DF:
    csv_files = glob(TS_RESPONSES_REGEX_PATH)
    # Read them all into a list
    df_list = [pd.read_csv(file, parse_dates=["readable_date"]) for file in csv_files]
    return (
        pd.concat(df_list, ignore_index=True, axis="index")
        .rename(columns={"readable_date": "date"})
        .astype(DATA_TYPE_RAW_DF_DICT)
        .drop_duplicates(subset=["vin",  "date"])
        .sort_values(by=["vin",  "date"])
    )    

if __name__ == "__main__":
    main()
