import glob

import pandas as pd
from pandas import DataFrame as DF

from transform.watea.watea_config import *
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result

@cache_result(RAW_TS_PATH, on="local")
def get_raw_tss() -> DF:
    print("called")
    raw_tss = {}
    for path in glob.iglob(WATEA_RESPONSES_REGEX, recursive=True):
        split_path = path.split("/")
        id = split_path[-2][-6:]
        raw_tss[id] =  (
            pd.read_parquet(path)
            .assign(id=id)
            .astype({"id":"string"})
        )

    raw_tss = pd.concat(raw_tss, ignore_index=True)
    
    return raw_tss

if __name__ == '__main__':
    single_dataframe_script_main(get_raw_tss, force_update=True)
