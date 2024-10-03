import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from logging import Logger, getLogger

from core.s3_utils import S3_Bucket
from core.constants import S3_RAW_TSS_KEY_FORMAT
from core.console_utils import single_dataframe_script_main
from core.caching_utils import singleton_s3_data_caching
from core.pandas_utils import concat

@singleton_s3_data_caching(S3_RAW_TSS_KEY_FORMAT.format(brand="BMW"))
def get_raw_tss(bucket: S3_Bucket, **kwargs) -> DF:
    logger = getLogger("transform.BMW-RawTSS")
    return (
        bucket.list_responses_keys_of_brand("BMW")
        .apply(parse_response_as_raw_ts, axis="columns", bucket=bucket, logger=logger)
        .pipe(concat)
    )

# Small work around for the raw_ts notebook
# TODO: Remove once we have the processed TS and use processed ts in the notebook (that will be called processed_ts_EDA I guess).
@singleton_s3_data_caching(S3_RAW_TSS_KEY_FORMAT.format(brand="BMW"))
def get_raw_tss_without_units(bucket: S3_Bucket) -> DF:
    logger = getLogger("BMW-RawTSS")
    return (
        bucket.list_responses_keys_of_brand("BMW")
        .apply(parse_response_as_raw_ts, axis="columns", bucket=bucket, logger=logger, add_units=True)
        .pipe(concat)
    )

def parse_response_as_raw_ts(key:Series, bucket:S3_Bucket, logger:Logger, add_units=True) -> DF:
    api_response = bucket.read_json_file(key["key"])                                # The json response contains a "data" key who's values are a list of dicts.
    raw_ts = DF.from_dict(api_response["data"])                                     # The dicts have a "key" and "value" items.
    if add_units:
        unit_not_none = raw_ts["unit"].notna()                                      # They also have a "unit" item but not all of them are not null.
        raw_ts.loc[unit_not_none, "key"] += "_" + raw_ts.loc[unit_not_none, "unit"] # So we append "_" + "unit" to the key only if the "unit" is not none.
    raw_ts = (
        raw_ts
        .pipe(
            pd.pivot_table,                                                     
            columns="key",
            values="value",
            index="date_of_value",
            aggfunc="first",
            dropna=False,
        )
        .assign(vin=key["vin"])
        .reset_index(drop=False)
    )

    logger.debug(f"\n{raw_ts}")

    return raw_ts


if __name__ == "__main__":
    single_dataframe_script_main(
        get_raw_tss,
        bucket=S3_Bucket(),
        force_update=True,
    )

