import logging

import pandas as pd
from pandas import DataFrame as DF

from core.pandas_utils import total_MB_memory_usage, floor_to
from core.console_utils import main_decorator
from core.caching_utils import singleton_data_caching
from core.time_series_processing import compute_cum_integrals_of_current_vars, perf_mask_and_idx_from_condition_mask
from analysis.tesla.tesla_constants import *
from analysis.tesla.tesla_raw_tss import get_raw_tss
from analysis.tesla.tesla_fleet_info import fleet_info_df


logger = logging.getLogger(__name__)

@main_decorator
def main():
    tss = get_processed_tss(force_update=True)
    print(tss)
    print(f"total memory usage: {total_MB_memory_usage(tss):.2f}MB")


@singleton_data_caching(PROCESSED_TSS_PATH)
def get_processed_tss() -> DF:
    return (
        get_raw_tss()
        .rename(columns={"readable_date": "date"})
        .astype(DATA_TYPE_RAW_DF_DICT)
        .drop_duplicates(subset=["vin",  "date"])
        .sort_values(by=["vin",  "date"])
        .rename(columns={
            "battery_level": "soc",
        })
        .eval("in_charge = charging_state == 'Charging'")
        .eval("in_discharge = charging_state == 'Disconnected'")
        .groupby("vin")
        .apply(process_ts, include_groups=False)
    )

def process_ts(raw_ts:DF) -> DF:
    vin = raw_ts.name
    return (
        raw_ts
        .assign(
            ffiled_outside_temp=raw_ts["outside_temp"].ffill(),
            ffiled_inside_temp=raw_ts["inside_temp"].ffill(),
            ffilled_odometer=raw_ts["odometer"].ffill(),
            floored_soc=floor_to(raw_ts["soc"].ffill(), 1),
            date_diff=raw_ts["date"].diff(),
            soc_diff=raw_ts["soc"].diff(),
            model=fleet_info_df.loc[vin, "model"],
            default_capacity=fleet_info_df.loc[vin, "default_kwh_energy_capacity"],
        )
        .sort_values(by="date")
        .pipe(compute_cum_integrals_of_current_vars)
        .pipe(perf_mask_and_idx_from_condition_mask, "in_charge")
        .pipe(perf_mask_and_idx_from_condition_mask, "in_discharge")
        .assign(energy_diff=lambda df: df["cum_energy"].diff())
    )

if __name__ == "__main__":
    main()
