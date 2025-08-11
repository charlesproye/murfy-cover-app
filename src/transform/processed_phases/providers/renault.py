from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.raw_results.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class RenaultRawTsToProcessedPhases(RawTsToProcessedPhases):

    def __init__(
        self,
        make="renault",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features_before_aggregation(self, df_tss):
        df_tss = df_tss.withColumn("expected_battery_energy",
                        F.when(
                            (F.col("net_capacity").isNotNull()) & (F.col("soc").isNotNull()),
                            F.col("net_capacity") * (F.col("soc") / 100.0)
                        )
                        .otherwise(None)
                    )
        return df_tss

    def aggregate_stats(self, df_tss):
        agg_columns = [
            # Minimum 
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            # Renault / A voir si je peux gérer ça avec la config
            F.sum('battery_energy').alias('BATTERY_ENERGY'),
            F.sum('expected_battery_energy').alias('EXPECTED_BATTERY_ENERGY'),
            F.mean('charging_rate').alias('CHARGING_RATE')
        ]

        if "consumption" in df_tss.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = (
            df_tss.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )

        df_aggregated = df_aggregated.withColumn("SOH", F.col("BATTERY_ENERGY") / F.col("EXPECTED_BATTERY_ENERGY"))

        return df_aggregated

    
    def compute_specific_features_after_aggregation(self, df_aggregated):

        df_aggregated = df_aggregated.withColumn("SOH", F.col("BATTERY_ENERGY") / F.col("EXPECTED_BATTERY_ENERGY"))

        df_aggregated = (
            df_aggregated.withColumn(
                "LEVEL_1",
                F.col("SOC_DIFF")
                * (F.col("CHARGING_RATE") < F.lit(LEVEL_1_MAX_POWER)).cast("int"),
            ) 
            .withColumn(
                "level_2",
                F.col("soc_diff")
                * F.col("CHARGING_RATE")
                .between(F.lit(LEVEL_1_MAX_POWER), F.lit(LEVEL_2_MAX_POWER))
                .cast("int"),
            )
            .withColumn(
                "level_3",
                F.col("soc_diff")
                * (F.col("CHARGING_RATE") > F.lit(LEVEL_2_MAX_POWER)).cast("int"),
            )
        )

        return df_aggregated
