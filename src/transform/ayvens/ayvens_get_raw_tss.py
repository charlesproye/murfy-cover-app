from dateutil import parser
from logging import Logger, getLogger

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.constants import *
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import instance_s3_data_caching
from core.pandas_utils import concat, uniques_as_series
from transform.ayvens.ayvens_fleet_info import fleet_info
from transform.config import *

@instance_s3_data_caching(S3_RAW_TSS_KEY_FORMAT.format(brand="ayvens"))
def get_ayvens_raw_tss(bucket: S3_Bucket=S3_Bucket()) -> DF:
    logger = getLogger(f"transform.Ayvens-RawTSS")
    makers = (
        fleet_info
        .query("activated == 'Found'")
        .loc[:, "make"]
        .pipe(uniques_as_series)
    )
    print(makers)
    return (
        makers
        .apply(get_raw_tss_of_brand, bucket=bucket)
        .pipe(concat)
    )

def get_raw_tss_of_brand(brand:str, bucket: S3_Bucket)  -> DF:
    print(brand)
    res = BRAND_PIPELINES.loc[brand, "raw_tss"](brand=brand, bucket=bucket)
    print(res)
    return res

if __name__ == "__main__":
    single_dataframe_script_main(get_ayvens_raw_tss, force_update=True)
