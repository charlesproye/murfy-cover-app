from pandas import DataFrame as DF

import core.time_series_processing as ts_proc
from core.caching_utils import cache_result
from core.console_utils import single_dataframe_script_main
from transform.watea.watea_raw_tss import get_raw_tss
from transform.watea.watea_config import *

@cache_result(PROCESSED_TS_PATH, on="local_storage")
def get_processed_tss() -> DF:
    tss = get_raw_tss()
    tss = tss.eval("power = current * voltage")
    tss = (
        tss
        .groupby("id")
        .apply(process_singe_ts, include_groups=False)
    )
    tss = tss.reset_index(level=0, drop=False)
    return tss

def process_singe_ts(ts:DF) -> DF:
    return (
        ts
        .assign(date=pd.to_datetime(ts["date"]).dt.as_unit("s"))
        .drop_duplicates("date")
        .set_index("date", drop=False)
        .sort_index()
        .pipe(set_sec_tim_diff)
        .pipe(ts_proc.high_freq_in_motion_mask_from_odo_diff)
        .pipe(ts_proc.high_freq_in_discharge_and_charge_from_soc_diff)
        # Ford E-Transit recordings tend to plateau at 99 of a random amout of time so remove these 
        .eval("in_charge = in_charge & soc < 99") 
        .pipe(ts_proc.perf_mask_and_idx_from_condition_mask, "in_charge", max_time_diff=PERF_MAX_TIME_DIFF)
        .pipe(ts_proc.perf_mask_and_idx_from_condition_mask, "in_discharge", max_time_diff=PERF_MAX_TIME_DIFF)
        .pipe(ts_proc.compute_cum_energy)
    )

def set_sec_tim_diff(ts: DF) -> DF:
    ts["sec_time_diff"] = (
        ts["date"]
        .ffill()
        .diff()
        .dt.as_unit("s")
        .astype("int32")
    )

    return ts


if __name__ == "__main__":
    tss = single_dataframe_script_main(
        get_processed_tss,
        force_update=True,
    )
