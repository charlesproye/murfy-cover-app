from os.path import splitext
from concurrent.futures import ThreadPoolExecutor

from logging import getLogger
from rich.progress import track

from core.pandas_utils import *
from core.s3_utils import S3_Bucket
from transform.raw_tss.config import *
from core.caching_utils import cache_result
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix


logger = getLogger(f"transform.Tesla-RawTSS")

@main_decorator
def main():
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    print(sanity_check(get_raw_tss(force_update=True)))

@cache_result(TESLA_RAW_TSS_KEY, on="s3")
def get_raw_tss(bucket: S3_Bucket = S3_Bucket()) -> DF:
    logger.debug("Getting raw tss from responses provided by tesla.")
    keys = get_response_keys_to_parse(bucket)
    new_raw_tss = get_raw_tss_from_keys(keys, bucket)
    if bucket.check_file_exists(TESLA_RAW_TSS_KEY):
        return concat([bucket.read_parquet_df(TESLA_RAW_TSS_KEY), new_raw_tss])
    else:
        return new_raw_tss

def get_response_keys_to_parse(bucket:S3_Bucket) -> DF:
    if bucket.check_file_exists(TESLA_RAW_TSS_KEY):
        raw_tss_subset = bucket.read_parquet_df(TESLA_RAW_TSS_KEY, columns=["vin", "readable_date"])
    else:
        raw_tss_subset = DEFAULT_TESLA_RAW_TSS_DF
    last_parsed_date = (
        raw_tss_subset
        .groupby("vin", observed=True, as_index=False)
        # Use "max" instead of "last" as the keys are not sorted
        .agg(last_parsed_date=pd.NamedAgg("readable_date", "max"))
    )
    return (
        bucket.list_responses_keys_of_brand("tesla")
        .assign(date=lambda df: df["file"].str[:-5].astype("datetime64[ns]"))
        .merge(last_parsed_date, "outer", "vin")
        .query("last_parsed_date.isna() | date > last_parsed_date") 
    )

def get_raw_tss_from_keys(keys:DF, bucket:S3_Bucket) -> DF:
    raw_tss = []
    for week, week_keys in track(keys.groupby(pd.Grouper(key='date', freq='W-MON'))):
        week_date = week.date().strftime('%Y-%m-%d')
        logger.debug(f"Parsing the responses of the week {week_date}:")
        logger.debug(f"{len(week_keys)} keys to parse for {week_keys['vin'].nunique()} vins.")
        logger.debug(f"This represents {len(week_keys) / len(keys):.2f}% of the total keys to parse.")
        responses = bucket.read_multiple_json_files(week_keys["key"].tolist(), max_workers=64)
        logger.debug(f"Read the responses.")
        with ThreadPoolExecutor(max_workers=64) as executor:
            week_raw_tss = list(executor.map(DF.from_records, responses))
        logger.debug(f"Parsed the responses:")
        week_raw_tss = concat(week_raw_tss)
        logger.debug(f"Concatenated the responses into a single DF.")
        base, ext = splitext(TESLA_RAW_TSS_KEY)
        bucket.save_df_as_parquet(week_raw_tss, f"{base}_{week_date}{ext}")
        logger.debug(f"Saved the DF at '{base}_{week_date}{ext}'.")
        raw_tss.append(week_raw_tss)
        logger.debug("")
    return concat(raw_tss, ignore_index=True) 

if __name__ == "__main__":
    main()

