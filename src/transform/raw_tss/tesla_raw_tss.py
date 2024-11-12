from logging import Logger, getLogger

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.config import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from core.pandas_utils import concat
from transform.raw_tss.config import *


@cache_result(S3_RAW_TSS_KEY_FORMAT.format(brand="tesla"), on="s3")
def get_raw_tss(bucket: S3_Bucket = S3_Bucket()) -> DF:
    logger = getLogger(f"transform.Tesla-RawTSS")
    return (
        bucket.list_responses_keys_of_brand("tesla")
        .apply(parse_response_as_raw_ts, axis="columns", bucket=bucket, logger=logger)
        .pipe(concat)
    )

def parse_response_as_raw_ts(key: Series, bucket:S3_Bucket, logger:Logger) -> DF:
    response = bucket.read_json_file(key["key"])
    if response is None:
        logger.debug(f"Did not parse key {key['key']} because the object returned by read_json_file was None.")
        return Series([])
    raw_ts = DF.from_records(response)
    raw_ts["vin"] = key["vin"]
    logger.debug(f"Parsed {key['key']} with raw_tss tesla parser.")

    return raw_ts

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    single_dataframe_script_main(get_raw_tss, force_update=True)
