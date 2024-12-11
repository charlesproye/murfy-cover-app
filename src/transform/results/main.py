from logging import getLogger

from core.sql_utils import *
from core.stats_utils import *
from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.results.config import *
from transform.results.renault_results import get_results as get_renault_results
from transform.results.ford_results import get_results as get_ford_results
from transform.results.tesla_results import get_results as get_tesla_results

logger = getLogger("transform.results.main")
GET_RESULTS_FUNCS = {
    "renault": get_renault_results,
    "tesla": get_tesla_results,
    "ford": get_ford_results,
}

def update_vehicle_data_table():
    logger.info("Updating 'vehicle_data' table.")
    return (
        get_all_processed_results()
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
    return pd.concat([get_processed_results(brand) for brand in GET_RESULTS_FUNCS.keys()])

def get_processed_results(brand:str) -> DF:
    results = GET_RESULTS_FUNCS[brand]()
    if brand == "ford" or brand == "renault":
        results = agg_results_by_discharge_and_charge(results)
    else:
        print("no aggregation...")
    return (
        results
        .dropna(subset=["soh", "odometer"], how="any")
        .reset_index()
        .groupby('vin')
        .apply(make_soh_presentable, include_groups=False)
        .reset_index(drop=False)  # Supprime l'index 'vin' créé par le groupby
        .assign(date=lambda df: df["date"].dt.floor(UPDATE_FREQUENCY))
        .groupby(["vin", "date"])
        .agg({
            "odometer": "last",    
            "soh": "median",
            "model": "first",
        })
        .pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS, logger=logger)
        .reset_index()
        .sort_values(["vin", "odometer"])
    )

def make_soh_presentable(df:DF) -> DF:
    if len(df) > 3:
        outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
        assert outliser_mask.sum() > 0, f"There seems to be only outliers???: {df['soh'].quantile(0.05)}, {df['soh'].quantile(0.95)}\n{df['soh']}"
        df = df[outliser_mask].copy()
    if len(df) > 2:
        df["soh"] = force_monotonic_decrease(df["soh"])
    return df

def agg_results_by_discharge_and_charge(results:DF) -> DF:
    return (
        results
        .groupby(["vin", "in_discharge_perf_idx"])
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
    single_dataframe_script_main(update_vehicle_data_table)

