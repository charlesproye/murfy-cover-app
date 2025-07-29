import datetime
from logging import getLogger
import asyncio
from core.sql_utils import *
from core.stats_utils import *
from core.pandas_utils import *
from transform.processed_results.config import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from pyspark.sql import functions as F, Window, SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import when, col, last, median, first, sum as spark_sum, lit
from transform.raw_results.RawResults import RawResult
from transform.processed_results.config import *
from core.spark_utils import create_spark_session
from pyspark.sql.types import DoubleType, DateType
from core.timer_utils import CodeTimer
from core.s3.settings import S3Settings
from core.s3.s3_utils import S3Service

class ProcessedResults():
    """
    Classe pour écrire les données de charge des véhicules électriques dans la base de données.
    """

    def __init__(self, make:str, spark:SparkSession=None):
        self.make = make
        self.spark=spark
    
    
    def run(self):
        logger.info(f"Démarrage de processed results pour {self.make}.")
        raw_results = RawResult(make=self.make, spark=self.spark).data.toPandas()
        processed_results = self.get_processed_results(raw_results, self.make)

        return processed_results


    def get_processed_results(self, raw_results:DF,brand:str) -> DF:
        logger.info(f"{'Processing ' + brand + ' results.':=^{50}}")


        with timer.step("Premier traitement"):
            results =  (
                raw_results
                # Some raw estimations may have inf values, this will make mask_out_outliers_by_interquartile_range and force_monotonic_decrease fail
                # So we replace them by NaNs.
                .assign(soh=lambda df: df["soh"].replace([np.inf, -np.inf], np.nan))
                .sort_values(["vin", "date"])
                .pipe(self._make_charge_levels_presentable)
                .eval(SOH_FILTER_EVAL_STRINGS[brand])
                .pipe(self._agg_results_by_update_frequency)
                .groupby('vin', observed=True)
                .apply(self._make_soh_presentable_per_vehicle, include_groups=False)
                .reset_index(level=0)
                #.pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS_LINE_BOUNDS, logger=logger)
                .sort_values(["vin", "date"])
            )
        
        with timer.step("Ajout de colonnes"):
            results["soh"] = results.groupby("vin", observed=True)["soh"].ffill()
            results["soh"] = results.groupby("vin", observed=True)["soh"].bfill()
            results["odometer"] = results.groupby("vin", observed=True)["odometer"].ffill()
            results["odometer"] = results.groupby("vin", observed=True)["odometer"].bfill()

        return results

    def _make_charge_levels_presentable(self, results:DF) -> DF:
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

    def _agg_results_by_update_frequency(self, results:DF) -> DF:
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
            )
        )
        

    def _make_soh_presentable_per_vehicle(self, df:DF) -> DF:
        if df["soh"].isna().all():
            return df
        if df["soh"].count() > 3:
            outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
            assert outliser_mask.any(), f"There seems to be only outliers???:\n{df['soh']}."
            df = df[outliser_mask].copy()
        if df["soh"].count() >= 2:
            df["soh"] = force_decay(df[["soh", "odometer"]])
        return df

    @classmethod
    def update_vehicle_data_table(cls, spark):
        processed_results = []
        logger.info("Updating 'vehicle_data' table.")
        for make in MAKES:
            cls = ProcessedResults
            processed_result = cls(make=make, spark=spark).run()
            processed_results.append(processed_result)
        return concat(processed_results)

if __name__ == "__main__":


    timer = CodeTimer("Pipeline de traitement")

    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")

    settings = S3Settings()

    # Initialisation
    bucket = S3Service()

    spark_session = create_spark_session(
        settings.S3_KEY,
        settings.S3_SECRET
    )

    df = ProcessedResults.update_vehicle_data_table(spark=spark_session)

    with timer.step("SQL operations"):
        df_global = left_merge_rdb_table(df, "vehicle", "vin", "vin", {"id": "vehicle_id"})
        truncate_rdb_table_and_insert_df(df_global, "vehicle_data", VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS, logger=logger)

    timer.get_summary()







