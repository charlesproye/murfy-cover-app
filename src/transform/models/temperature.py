import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from core.pandas_utils import *
from core.s3.s3_utils import S3Service
from core.spark_utils import (
    get_spark_available_cores,
    safe_astype_spark_with_error_handling,
)
from core.sql_utils import *
from core.stats_utils import *
from transform.processed_phases.config import (
    SOC_DIFF_THRESHOLD,
)
from transform.processed_phases.providers.renault import RenaultRawTsToProcessedPhases


class RenaultSOHModel:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.models = {}
        self.bucket_name = "bib-platform-prod-data"

    def load_data(self) -> DataFrame:
        renault = RenaultRawTsToProcessedPhases(
            make="renault",
            spark=self.spark,
        )

        tss = renault.bucket.read_parquet_df_spark(
            spark=self.spark,
            key=f"raw_ts/{renault.make.value}/time_series/raw_ts_spark.parquet",
        )
        tss = tss.repartition(1).cache()
        tss.count()

        optimal_partitions_nb, _ = renault._set_optimal_spark_parameters(
            tss, get_spark_available_cores(self.spark, renault.logger)
        )
        tss = tss.repartition(1).coalesce(optimal_partitions_nb)

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
        phases.repartition(1).cache()
        phases.count()

        phase_tss = renault.join_metrics_to_phase(phases, tss)
        phase_tss = renault.compute_specific_features_before_aggregation(phase_tss)

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

    def train(self, df: DataFrame):
        self.models = {}

        assembler = VectorAssembler(
            inputCols=["outside_temperature_filled"], outputCol="features"
        )
        df = assembler.transform(df)

        models = [row["model"] for row in df.select("model").distinct().collect()]

        for model_name in models:
            model_df = df.filter(F.col("model") == model_name)

            lr = LinearRegression(
                featuresCol="features",
                labelCol="soh",
                predictionCol="pred_soh",
            )
            lr_model = lr.fit(model_df)

            self.models[model_name] = lr_model

        return self.models

    def save_all(self):
        if not self.models:
            raise ValueError("No models to save. Train models first.")

        for model_name, model in self.models.items():
            save_path = f"s3a://{self.bucket_name}/models/{model_name}_temperature"
            model.write().overwrite().save(save_path)
            print(f"Model saved to {save_path}")

    def load_all(self, model_names: list[str]):
        self.models = {}

        for name in model_names:
            load_path = f"s3a://{self.bucket_name}/models/{name}_temperature"
            try:
                model = LinearRegressionModel.load(load_path)
                self.models[f"{name}_temperature"] = model
            except Exception:
                print(f"⚠️ Warning: model {name} not found in S3.")

        return self.models
