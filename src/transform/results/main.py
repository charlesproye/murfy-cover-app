import warnings
warnings.filterwarnings("error", category=RuntimeWarning)

from logging import getLogger

from core.sql_utils import *
from core.stats_utils import *
from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.results.config import *
from transform.results.ford_results import get_results as get_ford_results
from transform.results.volvo_results import get_results as get_volvo_results
from transform.results.tesla_results import get_results as get_tesla_results
from transform.results.renault_results import get_results as get_renault_results
from transform.results.mercedes_results import get_results as get_mercedes_results
from transform.results.odometer_aggregation import agg_last_odometer
from transform.results.stellantis_results import get_results as get_stellantis_results

logger = getLogger("transform.results.main")
GET_RESULTS_FUNCS = {
    "bmw": lambda: agg_last_odometer("bmw"),
    "kia": lambda: agg_last_odometer("kia"),
    "mercedes-benz": get_mercedes_results,
    "renault": get_renault_results,
    "tesla": get_tesla_results,
    "ford": get_ford_results,
    "volvo": get_volvo_results,
    "stellantis": get_stellantis_results,
}

def fill_vehicle_data_table_with_results():
    logger.info("Filling 'vehicle_data' table with results.")
    all = get_all_processed_results()
    print(all)
    return (
        all
        .pipe(left_merge_rdb_table, "vehicle", "vin", "vin", {"id": "vehicle_id"})
        .pipe(
            right_union_merge_rdb_table,
            "vehicle_data",
            left_on=["vehicle_id", "date"],
            right_on=["vehicle_id", "timestamp"],
            src_dest_cols=["soh", "odometer"]
        )
    )

def get_all_processed_results() -> DF:
    return (
        pd.concat([get_processed_results(brand) for brand in GET_RESULTS_FUNCS.keys()])
        .groupby("vin")
        .apply(add_lines_up_to_today_for_vehicle, include_groups=False)
        .reset_index()
    )

def add_lines_up_to_today_for_vehicle(results:DF) -> DF:
    last_date = pd.Timestamp.now().floor(UPDATE_FREQUENCY).date()
    dates_up_to_last_date = pd.date_range(results["date"].min(), last_date, freq=UPDATE_FREQUENCY, name="date")
    return (
        results
        .set_index("date")
        .sort_index()
        .reindex(dates_up_to_last_date, method="ffill")
    )

def get_processed_results(brand:str) -> DF:
    results = GET_RESULTS_FUNCS[brand]()
    logger.info(f"Processing results for {brand}.")
    if brand != "tesla":
        results = agg_results_by_update_frequncy(results)
    return (
        results
        .dropna(subset=["odometer"], how="any")
        .reset_index()
        .groupby('vin')
        .apply(make_soh_presentable, include_groups=False)
        .reset_index(drop=False)  # Supprime l'index 'vin' créé par le groupby
        .pipe(set_floored_day_date)
        .groupby(["vin", "date"])
        .agg({
            "odometer": "last",    
            "soh": "median",
            "model": "first",
            "version": "first",
        })
        .reset_index()
        .pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS_LINE_BOUNDS, logger=logger)
        .sort_values(["vin", "odometer"])
    )

def set_floored_day_date(df:DF, date_col:str="date") -> DF:
    df[date_col] = (
        pd.to_datetime(df[date_col], format='mixed')
        .dt.floor(UPDATE_FREQUENCY)
        .dt.tz_localize(None)
        .dt.date
        .astype('datetime64[ns]')
    )
    return df

def make_soh_presentable(df:DF) -> DF:
    if df["soh"].isna().all():
        return df
    if len(df) > 3:
        outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
        assert outliser_mask.sum() > 0, f"There seems to be only outliers???: {df['soh'].quantile(0.05)}, {df['soh'].quantile(0.95)}\n{df['soh']}"
        df = df[outliser_mask].copy()
    if len(df) >= 2:
        df["soh"] = force_monotonic_decrease(df["soh"])
    return df

def agg_results_by_update_frequncy(results:DF) -> DF:
    """
    Some results have the same amount of rows as the make's time series length.
    So we aggregate the results by update frequency to prevent memory overload.
    """
    return (
        results
        .eval("floored_date = date.dt.floor(@UPDATE_FREQUENCY)")
        .groupby(["vin", "floored_date"])
        .agg({
            "soh": "median",
            "odometer": "last",
            "date": "last",
            "date": "first",
            "model": "first",
            "version": "first",
        })
        .reset_index()
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(fill_vehicle_data_table_with_results)

