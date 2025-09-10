from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.result_phases.processed_phase_to_result_phase import ProcessedPhaseToResultPhase
from transform.result_phases.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER


class RenaultProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):

    def __init__(
        self,
        make="renault",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )


    def compute_soh(self, df_aggregated):
        """
        Compute the soh
        """
        return df_aggregated.withColumn("SOH", F.col("BATTERY_ENERGY") / F.col("EXPECTED_BATTERY_ENERGY"))

    
    def compute_charge_levels(self, df_aggregated):

        df_aggregated = (
            df_aggregated.withColumn(
                "LEVEL_1",
                F.col("SOC_DIFF")
                * (F.col("CHARGING_RATE") < F.lit(LEVEL_1_MAX_POWER)).cast("int"),
            ) 
            .withColumn(
                "LEVEL_2",
                F.col("SOC_DIFF")
                * F.col("CHARGING_RATE")
                .between(F.lit(LEVEL_1_MAX_POWER), F.lit(LEVEL_2_MAX_POWER))
                .cast("int"),
            )
            .withColumn(
                "LEVEL_3",
                F.col("SOC_DIFF")
                * (F.col("CHARGING_RATE") > F.lit(LEVEL_2_MAX_POWER)).cast("int"),
            )
        )

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
