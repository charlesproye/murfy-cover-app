from os.path import splitext
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import timedelta
from logging import getLogger
import pandas as pd
import dask.dataframe as dd
import numpy as np 

from rich.progress import track

from transform.raw_tss.config import *
from core.pandas_utils import concat, explode_data
from core.s3_utils import S3_Bucket
from core.caching_utils import cache_result
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main


logger = getLogger("transform.Tesla-fleet-telemetry-RawTSS")

@main_decorator
def main():
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    # print(sanity_check(get_raw_tss(force_update=True)))

@cache_result(FLEET_TELEMETRY_RAW_TSS_KEY, on="s3")
def get_raw_tss(bucket: S3_Bucket = S3_Bucket()) -> DF:
    logger.debug("Getting raw tss from responses provided by tesla fleet telemetry.")
    keys = get_response_keys_to_parse(bucket)
    if bucket.check_file_exists(FLEET_TELEMETRY_RAW_TSS_KEY):
        raw_tss, _ = bucket.read_parquet_df_dask(FLEET_TELEMETRY_RAW_TSS_KEY)
        #keys_to_parse = keys[keys['date'] >= pd.to_datetime((pd.to_datetime(raw_tss.readable_date.max()).date() - timedelta(days=1)))].copy()
        new_raw_tss = get_raw_tss_from_keys(keys, bucket)
        return concat([new_raw_tss, raw_tss])
    else:
        new_raw_tss = get_raw_tss_from_keys(keys, bucket)
        return new_raw_tss

def get_response_keys_to_parse(bucket:S3_Bucket) -> DF:
    if bucket.check_file_exists(FLEET_TELEMETRY_RAW_TSS_KEY):
        raw_tss_subset, _ = bucket.read_parquet_df_dask(FLEET_TELEMETRY_RAW_TSS_KEY, columns=["vin", "readable_date"])
    else:
        raw_tss_subset = DEFAULT_TESLA_RAW_TSS_DF
    last_parsed_date = (
        raw_tss_subset
        .groupby("vin", observed=True)
        # Use "max" instead of "last" as the keys are not sorted
        .agg(last_parsed_date=pd.NamedAgg("readable_date", "max"))
    ).compute()
    return (
        bucket.list_responses_keys_of_brand("tesla-fleet-telemetry")
        .assign(date=lambda df: df["file"].str[:-5].astype("datetime64[ns]"))
        .merge(last_parsed_date, "outer", "vin")
        .query("last_parsed_date.isna() | date > last_parsed_date")
    )

# def parse_and_explode(response):
#     df = DF.from_records(response)
#     return explode_data(df)

def get_raw_tss_from_keys(keys: DF, bucket: S3_Bucket, batch_size: int = 500) -> DF:
    raw_tss = []

    # Trier les clés pour un traitement reproductible
    keys = keys.sort_values("date")

    # Découpage en batches fixes
    key_batches = np.array_split(keys, len(keys) // batch_size + 1)

    for i, batch_keys in track(enumerate(key_batches), total=len(key_batches), description="Processing batches"):
        batch_date_range = f"{batch_keys['date'].min().date()} → {batch_keys['date'].max().date()}"
        logger.debug(f"[Batch {i+1}/{len(key_batches)}] Parsing {len(batch_keys)} responses:")
        logger.debug(f"- Vins: {batch_keys['vin'].nunique()}")
        logger.debug(f"- Date range: {batch_date_range}")
        logger.debug(f"- {round(len(batch_keys) / len(keys) * 100)}% of total keys")

        # Lecture + parsing combinée
        def load_and_parse(key):
            try:
                response = bucket.read_json(key)
                return explode_data(DF.from_records(response))
            except Exception as e:
                logger.warning(f"Failed to process key {key}: {e}")
                return pd.DataFrame()  # Skip on failure

        with ProcessPoolExecutor(max_workers=64) as executor:
            batch_dfs = list(executor.map(load_and_parse, batch_keys["key"].tolist()))

        batch_raw_tss = pd.concat(batch_dfs, ignore_index=True)
        raw_tss.append(batch_raw_tss)
        logger.debug(f"- Batch parsed and concatenated\n")

    return concat(raw_tss, ignore_index=True)

# def get_raw_tss_from_keys(keys:DF, bucket:S3_Bucket) -> DF:
#     raw_tss = []
#     grouped = keys.groupby(pd.Grouper(key='date', freq='W-MON'))
#     grouped_items = list(grouped)
#     for week, week_keys in track(grouped_items, description="Processing weekly groups"):
#         week_date = week.date().strftime('%Y-%m-%d')
#         logger.debug(f"Parsing the responses of the week {week_date}:")
#         logger.debug(f"{len(week_keys)} keys to parse for {week_keys['vin'].nunique()} vins.")
#         logger.debug(f"This represents {round(len(week_keys) / len(keys) * 100)}% of the total keys to parse.")
#         responses = bucket.read_multiple_json_files(week_keys["key"].tolist(), max_workers=64)
#         logger.debug(f"Read the responses.")
#         with ProcessPoolExecutor(max_workers=64) as executor:
#             week_raw_tss = list(executor.map(parse_and_explode, responses))
#         logger.debug(f"Parsed the responses:")
#         week_raw_tss = pd.concat(week_raw_tss).compute()
#         logger.debug(f"Concatenated the responses into a single DF.")
#         raw_tss.append(week_raw_tss)
#         logger.debug("")
#     return concat(raw_tss, ignore_index=True)

if __name__ == "__main__":
    main()
    single_dataframe_script_main(get_raw_tss, force_update=True)


