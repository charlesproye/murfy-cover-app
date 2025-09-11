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
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features_before_aggregation(self, phase_df):
        phase_df = phase_df.withColumn('battery_energy', F.col('battery_energy').cast('double'))

        phase_df_charging = phase_df.filter(F.col("PHASE_STATUS") == "charging")

        max_battery_energy = phase_df_charging.groupBy(["net_capacity", "soc"]).agg(
            F.expr("percentile_approx(battery_energy, 0.9)").alias("max_battery_energy")
        )

        df = phase_df.join(max_battery_energy, on=["net_capacity", "soc"], how="left")
        df = df.withColumn('max_battery_energy', F.col('max_battery_energy').cast('double'))

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
            # Ford specific features / Might be able to handle with config
            F.sum('battery_energy').alias('BATTERY_ENERGY_SUM'),
            F.sum('max_battery_energy').alias('MAX_BATTERY_ENERGY_SUM')
        ]


        if "consumption" in phase_df.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = (
            phase_df.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )

        return df_aggregated
