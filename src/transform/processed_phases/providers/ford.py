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

    def compute_specific_features_before_aggregation(self, df_tss):
        df_tss = df_tss.withColumn('battery_energy', F.col('battery_energy').cast('double'))

        df_tss_charging = df_tss.filter(F.col("PHASE_STATUS") == "charging")

        max_battery_energy = df_tss_charging.groupBy(["net_capacity", "soc"]).agg(
            F.expr("percentile_approx(battery_energy, 0.9)").alias("max_battery_energy")
        )

        df = df_tss.join(max_battery_energy, on=["net_capacity", "soc"], how="left")
        df = df.withColumn('max_battery_energy', F.col('max_battery_energy').cast('double'))

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
            # Ford / A voir si je peux gérer ça avec la config
            F.sum('battery_energy').alias('BATTERY_ENERGY_SUM'),
            F.sum('max_battery_energy').alias('MAX_BATTERY_ENERGY_SUM')
        ]

        

        if "consumption" in df_tss.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = (
            df_tss.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )


        return df_aggregated

    
    def compute_specific_features_after_aggregation(self, df_aggregated):

        df_aggregated = df_aggregated.withColumn(
            'BATTERY_ENERGY_SUM',
            F.when(F.col('PHASE_STATUS') == 'discharging', None).otherwise(F.col('BATTERY_ENERGY_SUM'))
        )
        df_aggregated = df_aggregated.withColumn(
            'MAX_BATTERY_ENERGY_SUM',
            F.when(F.col('PHASE_STATUS') == 'discharging', None).otherwise(F.col('MAX_BATTERY_ENERGY_SUM'))
        )

        df_aggregated = df_aggregated.withColumn("SOH", F.col("BATTERY_ENERGY_SUM") / F.col("MAX_BATTERY_ENERGY_SUM"))

        return df_aggregated
