from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.result_phases.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
from transform.result_phases.processed_phase_to_result_phase import ProcessedPhaseToResultPhase


class TeslaProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):

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

    
    def compute_specific_features(self, df_aggregated):

        df_aggregated = df_aggregated.withColumn("ENERGY_ADDED", F.col("ENERGY_ADDED_END") - F.col("ENERGY_ADDED_MIN"))

        return df_aggregated
    
    def compute_consumption(self, phase_df):
        """
        Compute the consumption
        """
        
        phase_df = (
            phase_df
            .withColumn("ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST"))
            .withColumn(
                "CONSUMPTION",
                F.when(F.col("PHASE_STATUS") == "discharging",
                (- 1 * F.col("SOC_DIFF"))
                * (F.col("BATTERY_NET_CAPACITY")) * (F.col("SOH") / 100)
                / F.col("ODOMETER_DIFF"),
                ).otherwise(None)
            )
        )

        return phase_df

    def compute_charge_levels(self, df_aggregated):
        return (
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
    
    def compute_cycles(self, df_aggregated):
        """
        Compute the cycles
        """

        return df_aggregated

    def compute_soh(self, df_aggregated):
        """
        Compute the SOH
        """

        df_aggregated = df_aggregated.withColumn(
            "SOH",
            F.col("ENERGY_ADDED") / (F.col("SOC_DIFF") / 100.0 * F.col("BATTERY_NET_CAPACITY")),
        )

        return df_aggregated
