from logging import getLogger
import asyncio
from core.sql_utils import *
from core.stats_utils import *
from core.pandas_utils import *
from transform.processed_results.config import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.processed_results.demo_data.ingest_demo import ingest_demo

logger = getLogger("transform.results.main")
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
    processed_results = []
    for brand in GET_RESULTS_FUNCS.keys():
        try:
            processed_results.append(get_processed_results(brand))
        except KeyboardInterrupt: 
            raise
        except:
            logger.error(f"Could not get processed results of {brand}.", exc_info=True)
    return concat(processed_results)

def get_processed_results(brand:str) -> DF:
    logger.info(f"{'Processing ' + brand + ' results.':=^{50}}")
    results =  (
        GET_RESULTS_FUNCS[brand]()
        # Some raw estimations may have inf values, this will make mask_out_outliers_by_interquartile_range and force_monotonic_decrease fail
        # So we replace them by NaNs.
        .assign(soh=lambda df: df["soh"].replace([np.inf, -np.inf], np.nan))
        .sort_values(["vin", "date"])
        .pipe(make_charge_levels_presentable)
        .eval(SOH_FILTER_EVAL_STRINGS[brand])
        .pipe(agg_results_by_update_frequency)
        .groupby('vin', observed=True)
        .apply(make_soh_presentable_per_vehicle, include_groups=True)
        .reset_index(drop=True)
        #.pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS_LINE_BOUNDS, logger=logger)
        .sort_values(["vin", "date"])
    )
    results["soh"] = results.groupby("vin", observed=True)["soh"].ffill()
    results["soh"] = results.groupby("vin", observed=True)["soh"].bfill()
    results["odometer"] = results.groupby("vin", observed=True)["odometer"].ffill()
    results["odometer"] = results.groupby("vin", observed=True)["odometer"].bfill()
    return results

# Raw results have different frequency, this function ensures that processed results all have the frequency
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
        .groupby(["vin", "date"], observed=True, as_index=False)
        .agg(
            odometer=pd.NamedAgg("odometer", "last"),
            soh=pd.NamedAgg("soh", "median"),
            model=pd.NamedAgg("model", "first"),
            version=pd.NamedAgg("version", "first"),
            level_1=pd.NamedAgg("level_1", "sum"),
            level_2=pd.NamedAgg("level_2", "sum"),
            level_3=pd.NamedAgg("level_3", "sum"), 
            consumption=pd.NamedAgg("consumption", "mean")           
        )
    )

def make_charge_levels_presentable(results:DF) -> DF:
    # If none of the level columns exist, return the results as is
    level_columns = ["level_1", "level_2", "level_3"]
    existing_level_columns = [col for col in level_columns if col in results.columns]
    if not existing_level_columns:
        return results
    negative_charge_levels = results[["level_1", "level_2", "level_3"]].lt(0)
    nb_negative_levels = negative_charge_levels.sum().sum()
    if nb_negative_levels > 0:
        logger.warning(f"There are {nb_negative_levels}({100*nb_negative_levels/len(results):2f}%) negative charge levels, setting them to 0.")
    results[["level_1", "level_2", "level_3"]] = results[["level_1", "level_2", "level_3"]].mask(negative_charge_levels, 0)
    return results

def make_soh_presentable_per_vehicle(df:DF) -> DF:
    if df["soh"].isna().all():
        return df
    if df["soh"].count() > 3:
        outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
        assert outliser_mask.any(), f"There seems to be only outliers???:\n{df['soh']}."
        df = df[outliser_mask].copy()
    if df["soh"].count() >= 2:
        print(df.columns)
        if df['vin'].iloc[0].startswith('VF1'):
            print('yeah')
            df["soh"] = force_decay(df[["soh", "odometer"]], max_drop=0.001)
        else:
            df["soh"] = force_decay(df[["soh", "odometer"]])
    return df

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(update_vehicle_data_table)

    ###Ingest demo data
    asyncio.run(ingest_demo())

