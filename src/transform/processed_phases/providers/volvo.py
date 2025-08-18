from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class VolvoRawTsToProcessedPhases(RawTsToProcessedPhases):

    def __init__(
        self,
        make="volvo-cars",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features_before_aggregation(self, df_tss):
        df_tss = df_tss.withColumn(
                        "SOH",
                        F.when(
                            (F.col("soc").isNotNull())
                            & (F.col("range").isNotNull())
                            & (F.col("range") != 0),
                            F.col("estimated_range")
                            / (F.col("soc") / 100.0)
                            / F.col("range")
                            / F.lit(0.87),
                        ).otherwise(None),
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
            # Volvo specific features / Might be able to handle with config
            F.expr("percentile_approx(SOH, 0.5)").alias('SOH')
        ]

        if "consumption" in df_tss.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = (
            df_tss.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )

        return df_aggregated
