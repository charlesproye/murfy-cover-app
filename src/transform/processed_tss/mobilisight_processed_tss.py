from logging import getLogger

from core.pandas_utils import *
from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.time_series_processing import compute_charging_n_discharging_masks, fillna_vars
from transform.processed_tss.config import *
from transform.raw_tss.mobilisight_raw_tss import get_raw_tss
from transform.fleet_info.main import fleet_info

logger = getLogger("transform.processed_tss.mobilisight_processed_tss")

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT, "s3", ["brand"])
def get_processed_tss(brand:str, **kwargs) -> DF:
    logger.info(f"get_processed_tss called for brand {brand}.")
    tss = get_raw_tss(brand, **kwargs)
    if tss.empty:
        logger.warning("tss is empty, returning it unchanged.")
        return tss
    return (
        tss
        .rename(columns=RENAME_COLS_DICT)
        .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()), logger=logger)
        .pipe(safe_astype, COL_DTYPES, logger=logger)
        .pipe(left_merge, fleet_info, "vin", "vin", COLS_TO_CPY_FROM_FLEET_INFO, logger)
        .sort_values(by=["vin", "date"])
        .pipe(compute_charging_n_discharging_masks, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK, logger=logger)
        .pipe(fillna_vars, COLS_TO_FILL, MAX_TIME_DIFF_TO_FILL, id_col="vin", logger=logger)
    )


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    single_dataframe_script_main(get_processed_tss, brand="peugeot", force_update=True)

