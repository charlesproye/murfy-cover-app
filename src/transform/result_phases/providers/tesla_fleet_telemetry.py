from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transform.result_phases.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
from transform.result_phases.processed_phase_to_result_phase import (
    ProcessedPhaseToResultPhase,
)


class TeslaFTProcessedPhaseToResultPhase(ProcessedPhaseToResultPhase):
    def __init__(
        self,
        make="tesla-fleet-telemetry",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_specific_features(self, df_aggregated):
        df_aggregated = df_aggregated.withColumn(
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

        return df_aggregated

    def compute_consumption(self, phase_df):
        """
        Compute the consumption
        """

        phase_df = phase_df.withColumn(
            "CONSUMPTION",
            F.when(
                (F.col("PHASE_STATUS") == "discharging") & (F.col("ODOMETER_DIFF") > 5),
                F.expr(
                    "try_divide(-1 * SOC_DIFF * BATTERY_NET_CAPACITY, ODOMETER_DIFF)"
                ),
            ).otherwise(None),
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

    def compute_soh(self, df_aggregated):
        """
        Compute the SOH
        """

        df_aggregated = df_aggregated.withColumn(
            "SOH",
            F.expr("try_divide(ENERGY_ADDED, SOC_DIFF / 100.0 * BATTERY_NET_CAPACITY)"),
        )

        return df_aggregated
