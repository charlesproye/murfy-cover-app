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

def get_ayvens_raw_tss(bucket: S3_Bucket=S3_Bucket()) -> dict[str, DF]:
    logger = getLogger(f"transform.Ayvens-RawTSS")
    print(fleet_info["activated"].unique())
    makers = (
        fleet_info
        .query("activated == 'activated'")
        .loc[:, "make"]
        .pipe(uniques_as_series)
    )
    print(makers)
    raw_tss = makers.apply(get_raw_tss_of_brand, bucket=bucket)
    return dict(zip(makers, raw_tss))


def get_raw_tss_of_brand(brand:str, bucket: S3_Bucket)  -> DF:
    brand_raw_tss = BRAND_PIPELINES.loc[brand, "raw_tss"](brand=brand, bucket=bucket)
    ayvens_vins_mask = brand_raw_tss["vin"].isin(fleet_info["vin"])
    brand_raw_tss = brand_raw_tss[ayvens_vins_mask]

    return brand_raw_tss

if __name__ == "__main__":
    yes = get_ayvens_raw_tss()
    print(yes)

