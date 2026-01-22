from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from core.spark_utils import compute_regression_coefficients
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

    def compute_specific_features_before_aggregation(self, phase_df):
        # Forward fill soh_oem: take last non-null value as it is a pushed data that won't change until next push
        window_ff = (
            Window.partitionBy("vin")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        phase_df = phase_df.withColumn(
            "soh_oem", F.last("soh_oem", ignorenulls=True).over(window_ff)
        )

        phase_df = phase_df.withColumn("soh_oem", F.col("soh_oem") / 100)

        phase_df = phase_df.withColumn(
            "remaining_capacity_kwh",
            F.col("remaining_capacity_kj").cast("double") / 3600,
        )

        df = phase_df.withColumn(
            "soh",
            F.when(
                (F.col("net_capacity") * F.col("soc")) != 0,
                F.col("remaining_capacity_kwh")
                * 100
                / (F.col("net_capacity") * F.col("soc")),
            )
            .otherwise(None)
            .cast("double"),
        )
        df = self._apply_soh_soc_correction(df)
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
            # KIA specific features
            F.expr("percentile_approx(soh_oem, 0.5)").alias("SOH_OEM"),
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

    def _apply_soh_soc_correction(self, phase_df):
        """
        Apply SoH correction based on linear regression between SoC and SoH.

        Args:
            phase_df: Spark DataFrame with columns 'VIN', 'soc', 'soh'

        Returns:
            Spark DataFrame with soh replaced by the corrected value
        """

        coef_df = compute_regression_coefficients(
            phase_df, aggregate_col="VIN", feature_col="soc", target_col="soh"
        )

        phase_df = phase_df.join(coef_df, on="VIN", how="left")

        phase_df = phase_df.withColumn(
            "soh_updated",
            F.when(
                F.col("coef").isNotNull(),
                F.col("soh") / (F.col("coef") * F.col("soc") + F.col("intercept")),
            ).otherwise(F.col("soh")),
        )

        phase_df = phase_df.drop("soh", "coef", "intercept").withColumnRenamed(
            "soh_updated", "soh"
        )

        return phase_df
