from dateutil import parser
import logging
import json
from datetime import datetime as DT

import pandas as pd
from pandas import DataFrame as DF
from pandas import Series

from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import instance_s3_data_caching
from core.pandas_utils import concat
from analysis.high_mobility.high_mobility_constants import *

@instance_s3_data_caching(HM_RAW_TSS_KEY_FORMAT, ["brand"])
def get_raw_tss(brand:str, bucket: S3_Bucket) -> DF:
    return (
        bucket.list_responses_keys_of_brand(brand)
        .apply(parse_response_as_raw_ts, axis="columns", bucket=bucket)
        .pipe(concat)
    )

def parse_response_as_raw_ts(key:Series, bucket:S3_Bucket, logger=logging.getLogger("HM_raw_tss")) -> DF:
    response = bucket.read_json_file(key["key"])
    if response is None:
        logger.info(f"Did not parse key {key['key']} because the object returned by read_json_file was None.")
        return Series([])
    flattened_response:dict = {}
    def flatten_dict(data, prefix=''):
        for key, value in data.items():
            new_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, list) and value and isinstance(value[0], dict):
                for item in value:
                    if 'datetime' in item:
                        timestamp = parser.isoparse(item['datetime'])
                        for sub_key, sub_value in item.items():
                            if sub_key != 'datetime':
                                variable_name = f"{new_key}.{sub_key}"
                                if isinstance(sub_value, dict) and not sub_value:
                                    # Handle empty dict by adding a dummy field
                                    flattened_response.setdefault(timestamp, {})[f"{variable_name}.dummy"] = None
                                else:
                                    flattened_response.setdefault(timestamp, {})[variable_name] = sub_value
            elif isinstance(value, dict):
                if not value:
                    # Handle empty dict at top level
                    flattened_response.setdefault(DT.now(), {})[f"{new_key}.dummy"] = None
                else:
                    flatten_dict(value, new_key)
            elif isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
                # Handle single-item lists like in the 'alerts' field
                flatten_dict(value[0], new_key)

    flatten_dict(response)

    raw_ts = (
        DF.from_dict(flattened_response, orient='index')
        .reset_index(names='date')
    )

    return raw_ts


if __name__ == "__main__":
    single_dataframe_script_main(
        get_raw_tss,
        bucket=S3_Bucket(),
        force_update=True,
        brand="stellantis",
    )

