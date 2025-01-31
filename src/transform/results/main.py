from logging import getLogger

from core.sql_utils import *
from core.stats_utils import *
from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.results.config import *
from transform.results.ford_results import get_results as get_ford_results
from transform.results.tesla_results import get_results as get_tesla_results
from transform.results.renault_results import get_results as get_renault_results
from transform.results.mercedes_results import get_results as get_mercedes_results
from transform.results.volvo_results import get_results as get_volvo_results
from transform.results.odometer_aggregation import agg_last_odometer

logger = getLogger("transform.results.main")
GET_RESULTS_FUNCS = {
    "mercedes-benz": get_mercedes_results,
    "bmw": lambda: agg_last_odometer("bmw"),
    "tesla": get_tesla_results,
    "kia": lambda: agg_last_odometer("kia"),
    "renault": get_renault_results,
    "volvo": get_volvo_results,
    "ford": get_ford_results,
}

def update_vehicle_data_table():
    logger.info("Updating 'vehicle_data' table.")
    return (
        get_all_processed_results()
        .pipe(left_merge_rdb_table, "vehicle", "vin", "vin", {"id": "vehicle_id"})
        .pipe(
            truncate_rdb_table_and_insert_df,
            "vehicle_data",
            src_dest_cols=VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS,
            logger=logger,
        )
    )

def get_all_processed_results() -> DF:
    return pd.concat([get_processed_results(brand) for brand in GET_RESULTS_FUNCS.keys()])

def get_processed_results(brand:str) -> DF:
    NB_SEP = 18
    log_end_sep = "=" * (NB_SEP - len(brand))
    logger.info(f"==================Processing {brand} results.{log_end_sep}")
    results = GET_RESULTS_FUNCS[brand]()
    results =  (
        results
        .sort_values(["vin", "date"])
        .pipe(agg_results_by_update_frequency)
        .pipe(make_charge_levels_presentable)
        .groupby('vin')
        .apply(make_soh_presentable_per_vehicle, include_groups=False)
        .reset_index(level=0)
        .pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS_LINE_BOUNDS, logger=logger)
        .sort_values(["vin", "date"])
    )
    results["soh"] = results.groupby("vin")["soh"].ffill()
    results["soh"] = results.groupby("vin")["soh"].bfill()
    results["odometer"] = results.groupby("vin")["odometer"].ffill()
    results["odometer"] = results.groupby("vin")["odometer"].bfill()

    return results

def agg_results_by_update_frequency(results:DF) -> DF:
    results["date"] = (
        pd.to_datetime(results["date"], format='mixed')
        .dt.floor(UPDATE_FREQUENCY)
        .dt.tz_localize(None)
        .dt.date
        .astype('datetime64[ns]')
    )
    return (
        results
        # Setting level columns to 0 if they don't exist.
        .assign(
            level_1=results.get("level_1", 0),
            level_2=results.get("level_2", 0),
            level_3=results.get("level_3", 0),
        )
        .groupby(["vin", "date"])
        .agg(
            odometer=pd.NamedAgg("odometer", "last"),
            soh=pd.NamedAgg("soh", "median"),
            model=pd.NamedAgg("model", "first"),
            version=pd.NamedAgg("version", "first"),
            level_1=pd.NamedAgg("level_1", "sum"),
            level_2=pd.NamedAgg("level_2", "sum"),
            level_3=pd.NamedAgg("level_3", "sum"),
        )
        .reset_index()
    )

def make_charge_levels_presentable(results:DF) -> DF:
    negative_charge_levels = results[["level_1", "level_2", "level_3"]].lt(0)
    nb_negative_levels = negative_charge_levels.sum().sum()
    if nb_negative_levels > 0:
        logger.debug(f"There are {nb_negative_levels}({100*nb_negative_levels/len(results):2f}%) negative charge levels, setting them to 0.")
    results[["level_1", "level_2", "level_3"]] = results[["level_1", "level_2", "level_3"]].mask(negative_charge_levels, 0)
    return results

def make_soh_presentable_per_vehicle(df:DF) -> DF:
    if df["soh"].isna().all():
        return df
    if df["soh"].count() > 3:
        outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
        assert outliser_mask.sum() > 0, f"There seems to be only outliers???: {df['soh'].quantile(0.05)}, {df['soh'].quantile(0.95)}\n{df['soh']}"
        df = df[outliser_mask].copy()
    if df["soh"].count() >= 2:
        df["soh"] = force_monotonic_decrease(df["soh"]).values
    return df

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(update_vehicle_data_table)

