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
    #"bmw": lambda: agg_last_odometer("bmw"),
    #"kia": lambda: agg_last_odometer("kia"),
    #"mercedes-benz": get_mercedes_results,
    "renault": get_renault_results,
    "tesla": lambda : pd.read_csv("tesla_results.csv"), # get_tesla_results,
    #"volvo": get_volvo_results,
    #"ford": get_ford_results,
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
            src_dest_cols=["soh", "odometer", "level_1", "level_2", "level_3"]
        )
    )

def get_all_processed_results() -> DF:
    return (
        pd.concat([get_processed_results(brand) for brand in GET_RESULTS_FUNCS.keys()])
    )

def get_processed_results(brand:str) -> DF:
    results = GET_RESULTS_FUNCS[brand]()
    logger.info(f"Processing {brand} results.")
    results =  (
        results
        .sort_values(["vin", "date"])
        .pipe(agg_results_by_update_frequency)
        .groupby('vin')
        .apply(make_soh_presentable, include_groups=False)
        .pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS_LINE_BOUNDS, logger=logger)
        .groupby("vin")
        .apply(add_lines_up_to_today_for_single_vehicle, include_groups=False)
        .reset_index()
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

def make_soh_presentable(df:DF) -> DF:
    if df["soh"].isna().all():
        #logger.warning(f"No SOH data for {df.name}")
        return df
    if len(df) > 3:
        outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
        assert outliser_mask.sum() > 0, f"There seems to be only outliers???: {df['soh'].quantile(0.05)}, {df['soh'].quantile(0.95)}\n{df['soh']}"
        df = df[outliser_mask].copy()
    if len(df) >= 2:
        df["soh"] = force_monotonic_decrease(df["soh"])
    return df

def add_lines_up_to_today_for_single_vehicle(results:DF) -> DF:
    last_date = pd.Timestamp.now().floor(UPDATE_FREQUENCY).date()
    dates_up_to_last_date = pd.date_range(results["date"].min(), last_date, freq=UPDATE_FREQUENCY, name="date")
    return (
        results
        .set_index("date")
        .sort_index()
        .reindex(dates_up_to_last_date, method="ffill")
    )
    

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(update_vehicle_data_table)

