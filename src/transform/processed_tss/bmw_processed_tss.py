from logging import getLogger

from core.pandas_utils import *
from core.time_series_processing import compute_charging_n_discharging_masks
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
    logger.debug(f"Sanity check for raw TSS from BMW:\n{sanity_check(raw_tss_from_bmw)}")
    logger.debug("Processing BNW raw TSS from BMW...")
    tss_from_bmw = (
        raw_tss_from_bmw
        .drop(columns=["date_of_value"])
        .pipe(process_raw_tss_provided_by_bmw)
    )
    logger.debug("Done.")
    logger.debug("Processing BNW raw TSS from high mobility...")
    logger.debug(f"Sanity check for raw TSS from high mobility:\n{sanity_check(raw_tss_from_high_mobility)}")
    tss_from_high_mobility = (
        raw_tss_from_high_mobility
        .drop(columns=["date"])
        .pipe(hm_process_raw_tss)
        .pipe(dropna_cols)
    )
    logger.debug("Done.")
    logger.debug(f"Sanity check for processed TSS from BMW:\n{sanity_check(tss_from_bmw)}")
    logger.debug(f"Sanity check for processed TSS from high mobility:\n{sanity_check(tss_from_high_mobility)}")
    logger.debug("Concatenating processed TSS from BMW and high mobility...")
    return pd.concat((tss_from_bmw, tss_from_high_mobility))

def process_raw_tss_provided_by_bmw(raw_tss:DF) -> DF:
    tss = (
        raw_tss
        .rename(columns=RENAME_COLS_DICT, errors="ignore")
        .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()))
        .pipe(safe_astype, COL_DTYPES)
        .sort_values(by=["vin", "date"])
        .pipe(merge_with_columns, fleet_info, COLS_TO_CPY_FROM_FLEET_INFO, merge_on="vin")
        .pipe(compute_charging_n_discharging_masks, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK)
        .pipe(dropna_cols)
    )

    return tss

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    single_dataframe_script_main(get_processed_tss, force_update=True, logger=logger)

