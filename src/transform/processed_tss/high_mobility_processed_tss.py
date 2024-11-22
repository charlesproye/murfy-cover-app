from logging import getLogger
from logging import Logger

from core.pandas_utils import *
from core.time_series_processing import compute_charging_n_discharging_masks
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

def process_raw_tss(tss:DF, logger:Logger=logger) -> DF:
    if tss.empty:
        logger.warning("tss is empty, returning it unchanged.")
        return tss
    tss = tss.rename(columns=RENAME_COLS_DICT)
    tss = safe_astype(tss, COL_DTYPES, logger=logger)
    tss = safe_locate(tss, col_loc=list(COL_DTYPES.keys()), logger=logger)
    tss = left_merge(tss, fleet_info, "vin", "vin", COLS_TO_CPY_FROM_FLEET_INFO, logger)
    tss = tss.sort_values(by=["vin", "date"])
    tss = compute_charging_n_discharging_masks(tss, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK, logger=logger)

    return tss

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    for brand in HIGH_MOBILITY_BRANDS:
        print("=================", brand, "=================")
        single_dataframe_script_main(get_processed_tss, brand=brand, force_update=True)

