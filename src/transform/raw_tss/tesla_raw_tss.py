from logging import Logger, getLogger
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress

from core.pandas_utils import *
from transform.raw_tss.config import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3_utils import S3_Bucket
from core.caching_utils import cache_result

logger = getLogger(f"transform.Tesla-RawTSS")

@cache_result(S3_RAW_TSS_KEY_FORMAT.format(brand="tesla"), on="s3")
def get_raw_tss(bucket: S3_Bucket = S3_Bucket()) -> DF:
    logger.debug("Getting raw tss from responses provided by tesla.")
    keys = bucket.list_responses_keys_of_brand("tesla")
    
    results = []
    with Progress() as progress:
        task = progress.add_task("Parsing responses...", visible=False, total=len(keys))
        with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers based on your system's capacity
            futures = {
                executor.submit(parse_response_as_raw_ts, key, bucket, logger, progress, task): key
                for _, key in keys.iterrows()
            }

            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    results.append(result)

    return concat(results)

def parse_response_as_raw_ts(key: Series, bucket: S3_Bucket, logger: Logger, progress: Progress, task: int) -> DF:
    progress.update(task, visible=True, advance=1, description=f"{key['vin']} {key['file']}")
    response = bucket.read_json_file(key["key"])
    if response is None:
        logger.debug(f"Did not parse key {key['key']} because the object returned by read_json_file was None.")
        return DF()
    raw_ts = DF.from_records(response)
    raw_ts["vin"] = key["vin"]

    return raw_ts

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    get_raw_tss(force_update=True)
