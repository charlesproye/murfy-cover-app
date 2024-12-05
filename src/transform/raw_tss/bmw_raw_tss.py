from logging import Logger, getLogger

from core.pandas_utils import *
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from transform.raw_tss.config import S3_RAW_TSS_KEY_FORMAT
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from transform.raw_tss.high_mobility_raw_tss import get_raw_tss as hm_get_raw_tss


logger:Logger = getLogger("transform.raw_tss.bmw_raw_tss")

@cache_result(S3_RAW_TSS_KEY_FORMAT.format(brand="BMW"), on="s3")
def get_raw_tss(bucket:S3_Bucket=bucket) -> DF:
    logger.debug("Getting raw tss frin responses provided by bmw.")
    return (
        bucket.list_responses_keys_of_brand("BMW")
        .groupby("vin")
        .apply(parse_responses_of_vin, include_groups=False)
        .reset_index(drop=False)
    )

def parse_responses_of_vin(responses:DF) -> DF:
    logger.debug(f"Parsing direct bmw responses of vin {responses.name}.")
    responses_dicts = responses["key"].apply(bucket.read_json_file)
    cat_responses_dicts = reduce(lambda cat_rep, rep_2: cat_rep + rep_2["data"], responses_dicts, [])
    return (
        DF.from_dict(cat_responses_dicts)
        .drop(columns=["unit", "info"])
        .eval("date_of_value = date_of_value.ffill().bfill()")
        .drop_duplicates(subset=["date_of_value", "key"])
        .pivot(index="date_of_value", columns="key", values="value")
    )


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    single_dataframe_script_main(
        get_raw_tss,
        force_update=True,
    )

