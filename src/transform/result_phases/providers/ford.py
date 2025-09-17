from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.result_phases.processed_phase_to_result_phase import ProcessedPhaseToResultPhase


class FordProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):

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
    
    def compute_specific_features_after_aggregation(self, df_aggregated):

        df_aggregated = df_aggregated.withColumn(
            'BATTERY_ENERGY_SUM',
            F.when(F.col('PHASE_STATUS') == 'discharging', None).otherwise(F.col('BATTERY_ENERGY_SUM'))
        )
        df_aggregated = df_aggregated.withColumn(
            'MAX_BATTERY_ENERGY_SUM',
            F.when(F.col('PHASE_STATUS') == 'discharging', None).otherwise(F.col('MAX_BATTERY_ENERGY_SUM'))
        )

        return df_aggregated

    def compute_soh(self, df_aggregated):
        """
        Compute the soh
        """
        return df_aggregated.withColumn("SOH", F.col("BATTERY_ENERGY_SUM") / F.col("MAX_BATTERY_ENERGY_SUM"))
