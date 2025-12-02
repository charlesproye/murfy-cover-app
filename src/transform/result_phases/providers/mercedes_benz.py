from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transform.result_phases.processed_phase_to_result_phase import (
    ProcessedPhaseToResultPhase,
)


class MercedesBenzProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):
    def __init__(
        self,
        make="mercedes-benz",
        spark: SparkSession = None,  # type: ignore[assignment]
        force_update: bool = False,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features(self, pph: DataFrame) -> DataFrame:
        """
        Compute the specific features
        """

        # La ligne du dessous mais en spark
        pph = pph.withColumn(
            "PHASE_DURATION",
            (F.col("DATETIME_END").cast("long") - F.col("DATETIME_BEGIN").cast("long"))
            / 3600,
        )

        pph = pph.withColumn(
            "ESTIMATED_CAPACITY",
            F.expr("try_divide(PHASE_DURATION * CHARGING_RATE_MEAN, SOC_DIFF / 100.0)"),
        )

        return pph

    def compute_soh(self, df_aggregated: DataFrame) -> DataFrame:
        """
        Compute the SOH
        """

        # Filters cf notebook/mercedes/Soh estimation/soh_estimation_mercedes.ipynb
        valid_charging_phase_mask = (
            (F.col("SOC_LAST") < 99)
            & (F.col("PHASE_DURATION") < 10)
            & (F.col("PHASE_STATUS") == "charging")
            & (F.col("NO_SOC_DATAPOINT") > 10)
            & (F.col("CHARGING_RATE_MEAN") > 0)
        )

        df_aggregated = df_aggregated.withColumn(
            "SOH",
            F.when(
                valid_charging_phase_mask,
                F.expr("try_divide(ESTIMATED_CAPACITY, BATTERY_NET_CAPACITY)"),
            ).otherwise(F.lit(None)),
        )

        return df_aggregated
