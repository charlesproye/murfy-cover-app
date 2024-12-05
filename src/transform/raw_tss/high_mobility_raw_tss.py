from dateutil import parser
from logging import Logger, getLogger

from core.pandas_utils import *
from rich.progress import Progress
from transform.raw_tss.config import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from core.pandas_utils import concat

@cache_result(S3_RAW_TSS_KEY_FORMAT, path_params=["brand"], on="s3")
def get_raw_tss(brand:str, bucket: S3_Bucket=S3_Bucket()) -> DF:
    logger = getLogger(f"transform.HighMobility-{brand}-RawTSS")
    logger.debug(f"Starting to compute raw time series for {brand} vehicles.")
    keys = bucket.list_responses_keys_of_brand(brand)
    if keys.empty:
        logger.warning(f"No keys found for brand '{brand}'.\n Returning empty dataframe.")
        return DF()
    with Progress() as progress:
        task = progress.add_task("Parsing responses...", visible=False, total=len(keys))
        return (
            keys
            .apply(parse_response_as_raw_ts, axis="columns", bucket=bucket, logger=logger, progress=progress, task=task)
            .pipe(concat)
        )

def parse_response_as_raw_ts(key:Series, bucket:S3_Bucket, logger:Logger, progress:Progress, task:int) -> DF:
    # The responses are first indexed by "capability" (see any high mobility's air table data catalog).
    # We don't really need to know what capability we are reading from.
    # But some variables that belong to different capabilities may have the same name.
    # To differentiate them, we will prepend their correspomding capability to their name.
    progress.update(task, visible=True, advance=1, description=f"key: {key['key']}, vin: {key['vin']}")
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

    return raw_ts


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    single_dataframe_script_main(
        get_raw_tss,
        bucket=S3_Bucket(),
        force_update=True,
        brand="ford",
    )
