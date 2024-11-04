from logging import getLogger

from core.pandas_utils import *
from core.caching_utils import cache_result
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.time_series_processing import compute_charging_n_discharging_masks
from core.config import S3_PROCESSED_TSS_KEY_FORMAT
from transform.processed_tss.config import *
from transform.raw_tss.main import get_raw_tss

logger = getLogger("transform.processed_tss.stellantis_processed_tss")

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT, path_params=["brand"], on="s3")
def get_processed_tss(brand:str) -> DF:
    return (
        get_raw_tss(brand)
        .rename(columns=RENAME_COLS_DICT)
        .pipe(safe_astype, COL_DTYPES)
        .sort_values(by=["vin", "date"])
        .pipe(print_data)
        .pipe(compute_charging_n_discharging_masks, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK)
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    for brand in MOBILISIGHT_BRANDS:
        single_dataframe_script_main(get_processed_tss, brand=brand, force_update=True)

