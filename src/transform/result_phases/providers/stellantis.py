from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transform.result_phases.processed_phase_to_result_phase import (
    ProcessedPhaseToResultPhase,
)


class StellantisProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):
    def __init__(
        self,
        make="stellantis",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_consumption(self, phase_df):
        """
        Compute the consumption
        """

        phase_df = phase_df.withColumn(
            "CONSUMPTION",
            F.when(
                F.col("PHASE_STATUS") == "discharging",
                (-1 * F.col("SOC_DIFF"))
                * (F.col("BATTERY_NET_CAPACITY"))
                * (F.col("SOH_OEM") / 100)
                / F.col("ODOMETER_DIFF"),
            ).otherwise(None),
        )

        return phase_df

