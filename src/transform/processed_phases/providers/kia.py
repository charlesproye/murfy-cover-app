from logging import Logger

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class KiaRawTsToProcessedPhases(RawTsToProcessedPhases):
    def __init__(
        self,
        make="kia",
        spark: SparkSession | None = None,
        force_update: bool = False,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def fill_forward(self, tss):
        w = (
            Window.partitionBy("vin")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        tss = (
            tss.withColumn(
                "remaining_capacity_kj",
                F.last("remaining_capacity_kj", ignorenulls=True).over(w),
            )
            .withColumn("soc", F.last("soc", ignorenulls=True).over(w))
            .withColumn("soh_oem", F.last("soh_oem", ignorenulls=True).over(w))
        )

        return tss

    def compute_specific_features_before_aggregation(self, phase_df):
        phase_df = phase_df.withColumn("soh_oem", F.col("soh_oem") / 100)

        phase_df = phase_df.withColumn(
            "remaining_capacity_kwh",
            F.col("remaining_capacity_kj").cast("double") / 3600,
        )

        df = phase_df.withColumn(
            "soh",
            F.col("remaining_capacity_kwh")
            * 100
            / (F.col("net_capacity") * F.col("soc")),
        )
        df = df.withColumn("soh", F.col("soh").cast("double"))

        return df

    def aggregate_stats(self, df_tss):
        agg_columns = [
            # Minimum
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            F.first("range", ignorenulls=True).alias("RANGE"),
            # KIA specific features
            F.expr("percentile_approx(soh_oem, 0.5)").alias("SOH_OEM"),
            F.expr("percentile_approx(soh, 0.5)").alias("SOH"),
        ]

        if "consumption" in df_tss.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = df_tss.groupBy(
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

