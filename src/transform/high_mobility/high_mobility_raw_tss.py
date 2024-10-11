from dateutil import parser
from logging import Logger, getLogger

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.config import *
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result_in_s3
from core.pandas_utils import concat

@cache_result_in_s3(S3_RAW_TSS_KEY_FORMAT, ["brand"])
def get_raw_tss(brand:str, bucket: S3_Bucket=S3_Bucket()) -> DF:
    logger = getLogger(f"transform.HighMobility-{brand}-RawTSS")
    keys = bucket.list_responses_keys_of_brand(brand)
    if keys.empty:
        logger.warning(f"No keys found for brand '{brand}'.\n Returning empty dataframe.")
        return DF()
    return (
        keys
        .apply(parse_response_as_raw_ts, axis="columns", bucket=bucket, logger=logger)
        .pipe(concat)
    )

def parse_response_as_raw_ts(key:Series, bucket:S3_Bucket, logger:Logger) -> DF:
    # The responses are first indexed by "capability" (see any high mobility's air table data catalog).
    # We don't really need to know what capability but some variables that belong to different capabilities may have the same name.
    # To differentiate them, we will prepend their correspomding capability to their name.
    response = bucket.read_json_file(key["key"])
    if response is None:
        logger.debug(f"Did not parse key {key['key']} because the object returned by read_json_file was None.")
        return Series([])
    flattened_response:dict = {}
    for capability, variables in response.items():
        if not isinstance(variables, dict):
            continue
        for variable, elements in variables.items():
            for element in elements:
                timestamp = parser.isoparse(element["timestamp"])
                variable_name = capability + "." + variable 
                if isinstance(element["data"], dict):
                    if not "value" in element["data"]:
                        continue
                    value = element["data"]["value"]
                    if "unit" in element:
                        variable_name += "." + element["unit"]
                else:
                    value = element["data"]
                flattened_response[timestamp] = flattened_response.get(timestamp, {}) | {variable_name: value}
    raw_ts = (
        DF.from_records(flattened_response)
        .T
        .reset_index(drop=False, names="date")
        .assign(vin=key["vin"])
    )

    logger.debug(f"Parsed {key['key']} with High mobility parsing.")

    return raw_ts


if __name__ == "__main__":
    single_dataframe_script_main(
        get_raw_tss,
        bucket=S3_Bucket(),
        force_update=True,
        brand="ford",
    )

