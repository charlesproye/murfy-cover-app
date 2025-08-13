from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class TeslaFTRawTsToProcessedPhases(RawTsToProcessedPhases):

    def __init__(
        self,
        make="tesla-fleet-telemetry",
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
            # TFT / A voir si je peux gérer ça avec la config
            F.min("ac_charge_energy_added").alias("AC_ENERGY_ADDED_MIN"),
            F.min("dc_charge_energy_added").alias("DC_ENERGY_ADDED_MIN"),
            F.last("ac_charge_energy_added", ignorenulls=True).alias(
                "AC_ENERGY_ADDED_END"
            ),
            F.last("dc_charge_energy_added", ignorenulls=True).alias(
                "DC_ENERGY_ADDED_END"
            ),
            F.expr("percentile_approx(ac_charging_power, 0.5)").alias(
                "AC_CHARGING_POWER_MEDIAN"
            ),
            F.expr("percentile_approx(dc_charging_power, 0.5)").alias(
                "DC_CHARGING_POWER_MEDIAN"
            ),
            F.first("tesla_code", ignorenulls=True).alias("TESLA_CODE"),
        ]
        

        if "consumption" in df_tss.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = (
            df_tss.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )


        return df_aggregated
