from logging import getLogger

from core.logging_utils import set_level_of_loggers_with_prefix
from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from transform.processed_tss.config import *
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.time_series_processing import *
from transform.processed_tss.config import *
from transform.raw_tss.tesla_raw_tss import get_raw_tss
from transform.fleet_info.main import fleet_info

logger = getLogger("transform.processed_tss.tesla")

from rich.progress import Progress

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT.format(brand="tesla"), on="s3")
def get_processed_tss(bucket: S3_Bucket = bucket) -> DF:
    with Progress(transient=True) as progress:
        raw_tss = get_raw_tss(bucket=bucket)
        task = progress.add_task("Processing VINs...", visible=False, total=raw_tss["vin"].nunique())
        return (
            raw_tss
            .rename(columns=RENAME_COLS_DICT, errors="ignore")
            .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()), logger=logger)
            .pipe(safe_astype, COL_DTYPES, logger=logger)
            .drop_duplicates(subset=["vin", "date"])
            .sort_values(by=["vin", "date"])
            .pipe(compute_charging_n_discharging, "vin", CHARGING_STATUS_VAL_TO_MASK, logger)
            .groupby("vin")
            .apply(lambda group: process_ts(group, progress, task), include_groups=False)
            .reset_index(drop=False)
            .pipe(set_all_str_cols_to_lower, but=["vin"])
            .pipe(left_merge, fleet_info.dropna(subset=["vin"]), "vin", "vin", COLS_TO_CPY_FROM_FLEET_INFO, logger)
            .pipe(compute_discharge_diffs, DISCHARGE_VARS_TO_MEASURE, logger)
        )

def process_ts(raw_ts: DF, progress: Progress, task) -> DF:
    vin = raw_ts.name
    progress.update(task, visible=True, advance=1, description=f"Processing vin {vin}...")
    if progress.finished:
        progress.update(task, visible=False)
    return (
        raw_ts
        .assign(
            ffiled_outside_temp=raw_ts["outside_temp"].ffill(),
            ffiled_inside_temp=raw_ts["inside_temp"].ffill(),
            floored_soc=floor_to(raw_ts["soc"].ffill(), 1),
            date_diff=raw_ts["date"].diff(),
            soc_diff=raw_ts["soc"].diff(),
        )
        .pipe(compute_cum_energy, power_col="power", cum_energy_col="cum_energy")
        .pipe(compute_cum_energy, power_col="charger_power", cum_energy_col="cum_charge_energy_added")
        .assign(energy_added=lambda tss: tss["cum_charge_energy_added"].diff())
        .assign(energy_diff=lambda df: df["cum_energy"].diff())
        .pipe(fillna_vars, COLS_TO_FILL, MAX_TIME_DIFF_TO_FILL)
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss.tesla")
    single_dataframe_script_main(get_processed_tss, force_update=True)
