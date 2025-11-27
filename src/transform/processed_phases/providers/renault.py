from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class RenaultRawTsToProcessedPhases(RawTsToProcessedPhases):
    def __init__(
        self,
        make="renault",
        spark: SparkSession | None = None,
        force_update: bool = False,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features_before_aggregation(self, phase_df):
        window_ff = (
            Window.partitionBy("vin")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        phase_df = phase_df.withColumn(
            "soc", F.last("soc", ignorenulls=True).over(window_ff)
        )

        # Cast columns to FloatType before arithmetic operations
        phase_df = phase_df.withColumn(
            "battery_energy", F.col("battery_energy").cast("float")
        )
        phase_df = phase_df.withColumn(
            "net_capacity", F.col("net_capacity").cast("float")
        )
        phase_df = phase_df.withColumn("soc", F.col("soc").cast("float"))

        phase_df = phase_df.withColumn(
            "soh",
            F.when(
                (F.col("net_capacity").isNotNull())
                & (F.col("battery_energy").isNotNull())
                & (F.col("soc").between(30, 99))
                & (F.col("IS_USABLE_PHASE") == F.lit(1)),
                F.try_divide(
                    F.col("battery_energy") * 100,
                    F.col("net_capacity") * F.col("soc"),
                ),
            ).otherwise(None),
        )

        return phase_df

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
            # Renault specific features / Might be able to handle with config
            F.expr("percentile_approx(soh, 0.5)").alias("SOH"),
            F.mean("charging_rate").alias("CHARGING_RATE"),
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
