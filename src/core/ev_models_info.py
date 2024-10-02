import pandas as pd
from pandas import Series
from pandas import DataFrame as DF

from core.caching_utils import singleton_data_caching
from core.constants import *

@singleton_data_caching(PARQUET_EV_MODELS_INFO_PATH)
def get_ev_models_infos() -> DF:
    return  (
        pd.read_csv(CSV_EV_MODELS_INFO_PATH)
        .astype({
            "model": "string",
            "manufacturer": "string",
            "kwh_capacity": "float",
        })
        .set_index("model", drop=False)
    )

