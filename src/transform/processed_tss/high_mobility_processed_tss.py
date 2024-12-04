from logging import getLogger
from logging import Logger

from core.pandas_utils import *
from core.time_series_processing import *
from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from transform.processed_tss.config import S3_PROCESSED_TSS_KEY_FORMAT
from transform.processed_tss.config import *
from transform.raw_tss.main import get_raw_tss
from transform.fleet_info.main import fleet_info

logger = getLogger("transform.processed_tss.hm_processed_tss")

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT, path_params=["brand"], on="s3")
def get_processed_tss(brand:str, **kwargs) -> DF:
    return (
        get_raw_tss(brand, **kwargs)
        .pipe(process_raw_tss)
    )

def process_raw_tss(raw_tss:DF, logger:Logger=logger) -> DF:
    if raw_tss.empty:
        logger.warning("tss is empty, returning it unchanged.")
        return raw_tss
    return (
        raw_tss
        .rename(columns=RENAME_COLS_DICT)
        .pipe(safe_astype, COL_DTYPES, logger=logger)
        .pipe(safe_locate, col_loc=COL_DTYPES.keys(), logger=logger)
        .pipe(left_merge, fleet_info, "vin", "vin", COLS_TO_CPY_FROM_FLEET_INFO, logger)
        .sort_values(by=["vin", "date"])
        .pipe(compute_charging_n_discharging, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK, logger=logger)
        .pipe(compute_discharge_diffs, DISCHARGE_VARS_TO_MEASURE, logger)
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    for brand in HIGH_MOBILITY_BRANDS:
        print("=================", brand, "=================")
        single_dataframe_script_main(get_processed_tss, brand=brand, force_update=True)

