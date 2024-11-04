from core.pandas_utils import *
from core.time_series_processing import compute_charging_n_discharging_masks
from core.caching_utils import cache_result
from core.console_utils import single_dataframe_script_main
from core.config import S3_PROCESSED_TSS_KEY_FORMAT
from transform.processed_tss.config import *
from transform.raw_tss.raw_tss import get_raw_tss
from transform.fleet_info.ayvens_fleet_info import fleet_info

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT, path_params=["brand"], on="s3")
def get_processed_tss(brand:str, **kwargs) -> DF:
    return (
        get_raw_tss(brand, **kwargs)
        .pipe(process_raw_tss)
    )

def process_raw_tss(raw_tss:DF) -> DF:
    return (
        raw_tss
        .rename(columns=RENAME_COLS_DICT)
        .pipe(safe_astype, COL_DTYPES)
        .pipe(merge_with_columns, fleet_info, COLS_TO_CPY_FROM_FLEET_INFO, merge_on="vin")
        .sort_values(by=["vin", "date"])
        .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()))
        .pipe(compute_charging_n_discharging_masks, id_col="vin", charging_status_val_to_mask=CHARGING_STATUS_VAL_TO_MASK)
    )

if __name__ == "__main__":
    for brand in HIGH_MOBILITY_BRANDS:
        print("=================", brand, "=================")
        single_dataframe_script_main(get_processed_tss, brand=brand, force_update=True)

