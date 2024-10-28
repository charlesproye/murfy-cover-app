import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from logging import Logger, getLogger

from core.s3_utils import S3_Bucket
from core.config import S3_RAW_TSS_KEY_FORMAT
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from core.pandas_utils import concat
from transform.raw_tss.high_mobility_raw_tss import get_raw_tss as hm_get_raw_tss

@cache_result(S3_RAW_TSS_KEY_FORMAT.format(brand="BMW"), on="s3")
def get_raw_tss(bucket: S3_Bucket) -> DF:
    return pd.concat((
        hm_get_raw_tss("bmw", bucket=bucket, force_update=True).assign(data_provider="high_mobility"),
        get_direct_bmw_raw_tss(bucket, append_units_to_col_names=False).assign(data_provider="bmw")
    ))

def get_direct_bmw_raw_tss(bucket: S3_Bucket, append_units_to_col_names=False) -> DF:
    logger = getLogger("transform.BMW-RawTSS")
    return (
        bucket.list_responses_keys_of_brand("BMW")
        .apply(
            parse_response_as_raw_ts,
            axis="columns",
            bucket=bucket,
            logger=logger,
            append_units_to_col_names=append_units_to_col_names
        )
        .pipe(concat)
    )

def parse_response_as_raw_ts(key:Series, bucket:S3_Bucket, logger:Logger, append_units_to_col_names=True) -> DF:
    api_response = bucket.read_json_file(key["key"])                                # The json response contains a "data" key who's values are a list of dicts.
    raw_ts = DF.from_dict(api_response["data"])                                     # The dicts have a "key" and "value" items.
    if append_units_to_col_names:
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

    logger.debug(f"Parsed {key['key']} with bmw parsing.")

    return raw_ts


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    single_dataframe_script_main(
        get_raw_tss,
        bucket=S3_Bucket(),
        force_update=True,
    )

