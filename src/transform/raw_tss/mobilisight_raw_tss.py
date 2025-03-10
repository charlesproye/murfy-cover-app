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

logger = getLogger("transform.raw_tss.mobilisight_raw_tss")

@cache_result(S3_RAW_TSS_KEY_FORMAT.format(brand="stellantis"), "s3")
def get_raw_tss(bucket:S3_Bucket=bucket) -> DF:
    logger.info(f"get_raw_tss called for Stellantis.")
    with Progress() as progress:
        mobilisight_responses_keys = bucket.list_responses_keys_of_brand("stellantis")
        task = progress.add_task("Parsing mobilisight responses", total=len(mobilisight_responses_keys))
        return (
            mobilisight_responses_keys
            .apply(
                parse_mobilisight_response,
                axis="columns",
                bucket=bucket,
                logger=logger,
                progress=progress,
                task=task,
            )
            .pipe(concat)
        )

def parse_mobilisight_response(response_key:str, bucket:S3_Bucket, logger:Logger=logger, progress:Progress=None, task=None) -> DF:
    progress.update(task, advance=1, description=f"key: {response_key['key']}, vin: {response_key['vin']}.")
    response = bucket.read_json_file(response_key['key'])
    return parse_unstructured_json(response, no_prefix_path=["datetime"], no_suffix_path=["value"]).assign(vin=response_key["vin"])

# def parse_responses_of_vin(responses:DF, bucket:S3_Bucket, logger:Logger, progress:Progress, task:int) -> DF:
#     progress.update(task, visible=True, advance=1, description=f"vin: {responses.name}")
#     response = bucket.read_json_file(responses["key"].iloc[0])
#     return parse_unstructured_json(
#         response, 
#         no_prefix_path=["datetime"], 
#         no_suffix_path=["value"]).assign(vin=responses.name)

# def parse_responses_of_vin(responses:DF, bucket:S3_Bucket, logger:Logger, progress:Progress, task:int) -> DF:
#     progress.update(task, visible=True, advance=1, description=f"vin: {responses.name}")
#     response = bucket.read_json_file(responses["key"].iloc[0])
    
#     # Transform the response into a format similar to BMW's data structure
#     return (
#         parse_unstructured_json(response, no_prefix_path=["datetime"], no_suffix_path=["value"])
#         .rename(columns={"datetime": "date_of_value"})  # Ensure consistent column naming
#         .set_index("date_of_value")  # Set the datetime as index
#         # .drop_duplicates() # Gives an error at the moment
#         # Add any necessary data cleaning steps here, similar to BMW's:
#         # .sort_index()
#     )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_tss")
    single_dataframe_script_main(get_raw_tss, force_update=True)

### Archives 
# @cache_result(S3_RAW_TSS_KEY_FORMAT, "s3", ["brand"])
# def get_raw_tss(brand:str, bucket:S3_Bucket=bucket) -> DF:
#     logger.info(f"get_raw_tss called for brand {brand}.")
#     mobilisght_responses_keys = bucket.list_responses_keys_of_brand("stellantis")
#     brand_responses_mask = mobilisght_responses_keys["vin"].isin(fleet_info.query(f"make == '{brand}'")["vin"])
#     brand_responses_keys = mobilisght_responses_keys[brand_responses_mask]
#     with Progress() as progress:
#         task = progress.add_task("Parsing mobilisight responses", total=len(brand_responses_keys))
#         return (
#             brand_responses_keys
#             .apply(
#                 parse_mobilisight_response,
#                 axis="columns",
#                 bucket=bucket,
#                 logger=logger,
#                 brand=brand,
#                 progress=progress,
#                 task=task,
#             )
#             .pipe(concat)
#         )

# def parse_mobilisight_response(response_key:str, bucket:S3_Bucket, logger:Logger=logger, brand:str="", progress:Progress=None, task=None) -> DF:
#     progress.update(task, advance=1, description=f"key: {response_key['key']}, vin: {response_key['vin']}.")
#     response = bucket.read_json_file(response_key['key'])
#     return parse_unstructured_json(response, no_prefix_path=["datetime"], no_suffix_path=["value"]).assign(vin=response_key["vin"])

