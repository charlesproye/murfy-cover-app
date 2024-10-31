from datetime import datetime as DT
import pytz

import pandas as pd
from pandas import DataFrame as DF
import plotly.express as px

from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.pandas_utils import safe_astype
from core.config import *
from core.pandas_utils import *
from transform.raw_tss.raw_tss import get_raw_tss
from transform.processed_tss.high_monility_processed_tss import process_raw_tss as hm_process_raw_tss
from transform.processed_tss.config import *

# For bmw we need to implement two processing steps.
# One for data comming from bmw and one for data comming from high mobility.

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT.format(brand="bmw"), on="s3")
def get_processed_tss() -> DF:
    raw_tss = get_raw_tss("bmw")
    raw_tss_from_bmw = raw_tss.query("data_provider == 'bmw'")
    raw_tss_from_high_mobility = (
        raw_tss
        .query("data_provider == 'high_mobility'")
        .drop(columns=["date"])
    )
    return pd.concat((
        process_raw_tss(raw_tss_from_bmw),
        hm_process_raw_tss(raw_tss_from_high_mobility)
    ))

def process_raw_tss(raw_tss:DF) -> DF:
    return (
        raw_tss
        .pipe(safe_astype, COL_DTYPES_BMW)
        .assign(date=pd.to_datetime(raw_tss["date_of_value"], format='mixed').mask(raw_tss["date_of_value"].isna(), raw_tss["date"]))
        .drop(columns=["date_of_value"])
        .rename(columns={
            "mileage": "odometer",
            "soc_hv_header": "soc",
        })
        .sort_values(by=["vin", "date"])
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    single_dataframe_script_main(get_processed_tss, force_update=True)

