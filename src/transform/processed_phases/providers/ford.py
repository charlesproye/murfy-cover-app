from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class FordRawTsToProcessedPhases(RawTsToProcessedPhases):
    def __init__(
        self,
        make="ford",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features_before_aggregation(self, phase_df):
        df = (
            phase_df.withColumn(
                "battery_energy", F.col("battery_energy").cast("double")
            )
            .withColumn("net_capacity", F.col("net_capacity").cast("double"))
            .withColumn("soc", F.col("soc").cast("double"))
            .withColumn(
                "soh",
                F.when(
                    (F.col("soc").between(40, 99))
                    & (F.col("IS_USABLE_PHASE") == F.lit(1)),
                    F.try_divide(
                        F.col("battery_energy") * 100,
                        F.col("net_capacity") * F.col("soc"),
                    ),
                ).otherwise(F.lit(None)),
            )
        )

        return df

    def aggregate_stats(self, phase_df):
        agg_columns = [
            # Minimum
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            F.first("range", ignorenulls=True).alias("RANGE"),
            # Ford specific features / Might be able to handle with config
            F.expr("percentile_approx(soh, 0.5)").alias("SOH"),
        ]

        if "consumption" in phase_df.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = phase_df.groupBy(
            "VIN",
            "PHASE_INDEX",
            "DATETIME_BEGIN",
            "DATETIME_END",
            "PHASE_STATUS",
            "SOC_FIRST",
            "SOC_LAST",
            "SOC_DIFF",
            "NO_SOC_DATAPOINT",
            "IS_USABLE_PHASE",
        ).agg(*agg_columns)

        return df_aggregated
