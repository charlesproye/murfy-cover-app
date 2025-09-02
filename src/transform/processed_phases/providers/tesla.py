from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class TeslaRawTsToProcessedPhases(RawTsToProcessedPhases):

    def __init__(
        self,
        make="tesla",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def aggregate_stats(self, df_tss):



        agg_columns = [
            # Minimum 
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            # Tesla specific features / Might be able to handle with config
            F.expr("percentile_approx(charging_power, 0.5)").alias('CHARGING_POWER'),
            F.first("tesla_code", ignorenulls=True).alias("TESLA_CODE"),
            F.min("charge_energy_added").alias("ENERGY_ADDED_MIN"),
            F.last("charge_energy_added").alias("ENERGY_ADDED_END"),
            F.mean('inside_temp').alias('INSIDE_TEMP_MEAN'),
            F.mean('outside_temp').alias('OUTSIDE_TEMP_MEAN'),
            F.first('range').alias('RANGE_FIRST')
        ]

        if "consumption" in df_tss.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = (
            df_tss.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )

        return df_aggregated
