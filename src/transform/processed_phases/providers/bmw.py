from logging import Logger

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class BMWRawTsToProcessedPhases(RawTsToProcessedPhases):
    def __init__(
        self,
        make="bmw",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features_before_aggregation(self, phase_df):
        phase_df = phase_df.withColumn("soh_oem", F.col("soh_oem") / 100)

        return phase_df

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
            # BMW specific features / Might be able to handle with config
            F.expr("percentile_approx(soh_oem, 0.5)").alias("SOH_OEM"),
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

