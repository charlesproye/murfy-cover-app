from os.path import dirname
from logging import getLogger

from rich.progress import Progress

from core.pandas_utils import *
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from transform.raw_tss.config import S3_RAW_TSS_KEY_FORMAT
from transform.fleet_info.main import fleet_info

logger = getLogger("transform.raw_tss.mobilisight_raw_tss")

@cache_result(S3_RAW_TSS_KEY_FORMAT, "s3", ["brand"])
def get_raw_tss(brand:str, bucket:S3_Bucket=bucket) -> DF:
    logger.info(f"get_raw_tss called for brand {brand}.")
    mobilisght_responses_keys = bucket.list_responses_keys_of_brand("stellantis")
    brand_responses_mask = mobilisght_responses_keys["vin"].isin(fleet_info.query(f"make == '{brand}'")["vin"])
    brand_responses_keys = mobilisght_responses_keys[brand_responses_mask]
    with Progress() as progress:
        task = progress.add_task("Parsing mobilisight responses", total=len(brand_responses_keys))
        return (
            brand_responses_keys
            .apply(
                parse_mobilisight_response,
                axis="columns",
                bucket=bucket,
                logger=logger,
                brand=brand,
                progress=progress,
                task=task,
            )
            .pipe(concat)
        )

def parse_mobilisight_response(response_key:str, bucket:S3_Bucket, logger:Logger=logger, brand:str="", progress:Progress=None, task=None) -> DF:
    progress.update(task, advance=1, description=f"key: {response_key['key']}, vin: {response_key['vin']}.")
    response = bucket.read_json_file(response_key['key'])
    return parse_unstructured_json(response, no_prefix_path=["datetime"], no_suffix_path=["value"]).assign(vin=response_key["vin"])


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_tss")
    single_dataframe_script_main(get_raw_tss, brand="peugeot", force_update=True)
