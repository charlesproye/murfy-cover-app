from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from core.models import MakeEnum
from core.spark_utils import (
    get_spark_available_cores,
    safe_astype_spark_with_error_handling,
)
from transform.models.base import BaseSOHModel


class RenaultSOHModel(BaseSOHModel):
    """SoH model trainer for Renault vehicles using temperature correction."""

    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, make=MakeEnum.renault)

    def get_feature_columns(self) -> list[str]:
        """Renault uses temperature correction for SoH estimation."""
        return ["outside_temperature_filled"]

    def load_data(self) -> DataFrame:
        from transform.processed_phases.config import (
            SOC_DIFF_THRESHOLD,
        )
        from transform.processed_phases.providers.renault import (
            RenaultRawTsToProcessedPhases,
        )

        renault = RenaultRawTsToProcessedPhases(
            make=self.make,
            spark=self.spark,
        )

        tss = renault.bucket.read_parquet_df_spark(
            spark=self.spark,
            key=f"raw_ts/{renault.make.value}/time_series/raw_ts_spark.parquet",
        )
        optimal_partitions_nb, _ = renault._set_optimal_spark_parameters(
            tss, get_spark_available_cores(self.spark, renault.logger)
        )

        tss = tss.coalesce(optimal_partitions_nb).cache()
        tss.count()

        tss = tss.withColumnsRenamed({"battery_level": "soc"})
        tss = safe_astype_spark_with_error_handling(tss)
        tss = renault._normalize_units_to_metric(tss)
        tss = tss.orderBy(["vin", "date"])
        tss = renault.fill_forward(tss)

        tss_phase_idx = renault.compute_charge_idx(
            tss, SOC_DIFF_THRESHOLD[renault.make.value]
        )
        tss_phase_idx = tss_phase_idx.cache()
        phases = renault.generate_phase(tss_phase_idx)
        phases.repartition(optimal_partitions_nb).cache()
        phases.count()

        phase_tss = renault.join_metrics_to_phase(phases, tss)
        phase_tss = renault.compute_specific_features_before_aggregation(
            phase_tss, apply_soh_correction=False
        )

        w_ffill = (
            Window.partitionBy("vin", "PHASE_INDEX")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        phase_tss = phase_tss.withColumn(
            "outside_temperature_ffill",
            F.last("outside_temperature", ignorenulls=True).over(w_ffill),
        )

        w_bfill = (
            Window.partitionBy("vin", "PHASE_INDEX")
            .orderBy(F.col("date").desc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        phase_tss = phase_tss.withColumn(
            "outside_temperature_bfill",
            F.last("outside_temperature", ignorenulls=True).over(w_bfill),
        )

        phase_tss = phase_tss.withColumn(
            "outside_temperature_filled",
            F.coalesce(
                F.col("outside_temperature_ffill"), F.col("outside_temperature_bfill")
            ).cast("double"),
        )

        phase_tss = phase_tss.filter(
            (F.col("soh") > 0) & (F.col("outside_temperature_ffill").isNotNull())
        ).select("vin", "date", "outside_temperature_filled", "soh", "model")

        return phase_tss
