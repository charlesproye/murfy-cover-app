from logging import getLogger

from core.pandas_utils import *
from core.time_series_processing import compute_charging_n_discharging_masks, fillna_vars
from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.config import *
from transform.fleet_info.main import fleet_info
from transform.raw_tss.main import get_raw_tss
from transform.processed_tss.high_mobility_processed_tss import process_raw_tss as hm_process_raw_tss
from transform.processed_tss.config import *

logger = getLogger("transform.processed_tss.bmw")

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT.format(brand="bmw"), on="s3")
def get_processed_tss() -> DF:
    raw_tss = get_raw_tss("bmw")
    raw_tss_from_bmw = raw_tss.query("data_provider == 'bmw'")
    raw_tss_from_high_mobility = raw_tss.query("data_provider == 'high_mobility'")
    logger.info("Processing BMW raw TSS from raw tss provided by BMW...")
    tss_from_bmw = (
        raw_tss_from_bmw
        .drop(columns=["date"])
        .pipe(process_raw_tss_provided_by_bmw)
    )
    logger.debug("Done.")
    logger.debug("Processing BMW raw TSS from raw tss provided by high mobility...")
    tss_from_high_mobility = (
        raw_tss_from_high_mobility
        .drop(columns=["date_of_value"])
        .pipe(hm_process_raw_tss)
        .pipe(dropna_cols)
    )
    logger.debug("Done.")
    logger.debug(f"Sanity check for processed TSS from BMW:\n{sanity_check(tss_from_bmw)}")
    logger.debug(f"Sanity check for processed TSS from high mobility:\n{sanity_check(tss_from_high_mobility)}")
    logger.debug("Concatenating processed TSS from BMW and high mobility...")
    return pd.concat((tss_from_bmw, tss_from_high_mobility))

def process_raw_tss_provided_by_bmw(tss:DF) -> DF:
    tss = tss.rename(columns=RENAME_COLS_DICT, errors="ignore")
    tss = safe_locate(tss, col_loc=list(COL_DTYPES.keys()), logger=logger)
    tss = safe_astype(tss, COL_DTYPES, logger=logger)
    tss = tss.sort_values(by=["vin", "date"])
    tss = left_merge(tss, fleet_info, left_on="vin", right_on="vin", src_dest_cols=COLS_TO_CPY_FROM_FLEET_INFO, logger=logger)
    tss = compute_charging_n_discharging_masks(tss, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK, logger=logger)
    tss = fillna_vars(tss, COLS_TO_FILL, MAX_TIME_DIFF_TO_FILL, id_col="vin", logger=logger)
    tss = dropna_cols(tss, logger=logger)

    return tss

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    single_dataframe_script_main(get_processed_tss, force_update=True, logger=logger)

