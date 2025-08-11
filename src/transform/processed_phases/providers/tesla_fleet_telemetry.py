from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases
from transform.processed_phases.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER


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

    
    def compute_specific_features_after_aggregation(self, df_aggregated):

        df = df_aggregated.withColumn(
            "CHARGING_POWER",
            F.coalesce(F.col("AC_CHARGING_POWER_MEDIAN"), F.lit(0))
            + F.coalesce(F.col("DC_CHARGING_POWER_MEDIAN"), F.lit(0)),
        )

        df_aggregated = (
            df_aggregated.withColumn(
                "AC_ENERGY_ADDED",
                F.col("AC_ENERGY_ADDED_END") - F.col("AC_ENERGY_ADDED_MIN"),
            )
            .withColumn(
                "DC_ENERGY_ADDED",
                F.col("DC_ENERGY_ADDED_END") - F.col("DC_ENERGY_ADDED_MIN"),
            )
            .withColumn("ENERGY_ADDED", F.col("DC_ENERGY_ADDED"))
        )

        df_aggregated = (
            df_aggregated.withColumn(
                "LEVEL_1",
                F.col("SOC_DIFF")
                * F.when(F.col("CHARGING_POWER") < LEVEL_1_MAX_POWER, 1).otherwise(0)
                / 100,
            )
            .withColumn(
                "LEVEL_2",
                F.col("SOC_DIFF")
                * F.when(
                    F.col("CHARGING_POWER").between(
                        LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
                    ),
                    1,
                ).otherwise(0)
                / 100,
            )
            .withColumn(
                "LEVEL_3",
                F.col("SOC_DIFF")
                * F.when(F.col("CHARGING_POWER") > LEVEL_2_MAX_POWER, 1).otherwise(0)
                / 100,
            )
        )

        return df_aggregated
