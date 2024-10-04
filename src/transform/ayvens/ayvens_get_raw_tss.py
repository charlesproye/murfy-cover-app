from dateutil import parser
from logging import Logger, getLogger

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.constants import *
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import instance_s3_data_caching
from core.pandas_utils import concat
from transform.ayvens.ayvens_fleet_info import fleet_info
from transform.config import *

@instance_s3_data_caching(S3_RAW_TSS_KEY_FORMAT.format(brand="ayvens"))
def get_ayvens_raw_tss(bucket: S3_Bucket=S3_Bucket()) -> DF:
    logger = getLogger(f"transform.Ayvens-RawTSS")
    return (
        fleet_info
        .query("Found == 'Found'")
        .groupby("make")
        .apply(get_raw_tss_of_brand, bucket)
        .pipe(concat)
    )

def get_raw_tss_of_brand(brand_fleet_info:str, bucket: S3_Bucket)  -> DF:
    return (
        BRAND_PIPELINES.loc[brand_fleet_info.name, "raw_tss"](brand=brand_fleet_info.name, bucket=bucket)
    )

# Pour toutes les marques presentes dans fleet_info:
    # get la raw_tss
    # get les vins present dans la marque
# Concat tout les raw tss

if __name__ == "__main__":
    single_dataframe_script_main(get_ayvens_raw_tss, force_update=True)
