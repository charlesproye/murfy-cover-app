from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transform.result_phases.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
from transform.result_phases.processed_phase_to_result_phase import (
    ProcessedPhaseToResultPhase,
)


class RenaultProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):
    def __init__(
        self,
        make="renault",
        spark: SparkSession | None = None,
        force_update: bool = False,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

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

        phase_df = phase_df.withColumn(
            "CONSUMPTION",
            F.when(
                (F.col("PHASE_STATUS") == "discharging") & (F.col("ODOMETER_DIFF") > 5),
                F.expr("try_divide(-SOC_DIFF * BATTERY_NET_CAPACITY, ODOMETER_DIFF)"),
            ).otherwise(None),
        )

        return phase_df
