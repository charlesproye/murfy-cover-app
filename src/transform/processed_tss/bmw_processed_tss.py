from logging import getLogger

from core.pandas_utils import *
from core.time_series_processing import *
from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import tss_script_main
from core.config import *
from transform.fleet_info.main import fleet_info
from transform.raw_tss.main import get_raw_tss
from transform.processed_tss.high_mobility_processed_tss import process_raw_tss as hm_process_raw_tss
from transform.processed_tss.config import *

logger = getLogger("transform.processed_tss.bmw")

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT.format(brand="bmw"), on="s3")
def get_processed_tss() -> DF:
    return (
        get_raw_tss("bmw")
        .rename(columns=RENAME_COLS_DICT, errors="ignore")
        .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()), logger=logger)
        .pipe(safe_astype, COL_DTYPES, logger=logger)
        .sort_values(by=["vin", "date"])
        .merge(fleet_info, left_on="vin", right_on="vin", how="left")
        .pipe(compute_charging_n_discharging_masks, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK, logger=logger)
        .pipe(dropna_cols, logger=logger)
        .pipe(compute_discharge_diffs, DISCHARGE_VARS_TO_MEASURE, logger)
    ) 

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    tss_script_main(get_processed_tss, force_update=True, logger=logger)

