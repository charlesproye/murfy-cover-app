from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from core.config import *
from core.s3_utils import S3_Bucket
from core.time_series_processing import compute_cum_energy, perf_mask_and_idx_from_condition_mask
from transform.processed_tss.config import *
from transform.raw_tss.tesla_raw_tss import get_raw_tss
from transform.tesla.tesla_fleet_info import fleet_info_df

@cache_result(S3_PROCESSED_TSS_KEY_FORMAT.format(brand="tesla"), on="s3")
def get_processed_tss(bucket: S3_Bucket = S3_Bucket()) -> DF:
    return (
        get_raw_tss(bucket=bucket)
        .rename(columns=RENAME_COLS_DICT, errors="ignore")
        .pipe(safe_astype, COL_DTYPES)
        .assign(date=lambda raw_tss: pd.to_datetime(raw_tss["date"]).dt.as_unit("s"))
        .drop_duplicates(subset=["vin",  "date"])
        .sort_values(by=["vin",  "date"])
        .eval("in_charge = charging_state == 'Charging'")
        .eval("in_discharge = charging_state == 'Disconnected'")
        .groupby("vin")
        .apply(process_ts, include_groups=False)
        .reset_index(drop=False)
        .sort_values(by=["vin", "date"])
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
        .pipe(compute_cum_energy)
        .pipe(perf_mask_and_idx_from_condition_mask, "in_charge")
        .pipe(perf_mask_and_idx_from_condition_mask, "in_discharge")
        .assign(energy_diff=lambda df: df["cum_energy"].diff())
    )

if __name__ == "__main__":
    single_dataframe_script_main(get_processed_tss, force_update=True)
