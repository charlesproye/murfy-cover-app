from logging import Logger

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transform.models.renault_soh_model import RenaultSOHModel
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases


class RenaultRawTsToProcessedPhases(RawTsToProcessedPhases):
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

    def compute_specific_features_before_aggregation(
        self, phase_df, apply_soh_correction: bool = True
    ):
        window_ff = (
            Window.partitionBy("vin")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        phase_df = phase_df.withColumn(
            "soc", F.last("soc", ignorenulls=True).over(window_ff)
        )

        # Cast columns to FloatType before arithmetic operations
        phase_df = phase_df.withColumn(
            "battery_energy", F.col("battery_energy").cast("float")
        )
        phase_df = phase_df.withColumn(
            "net_capacity", F.col("net_capacity").cast("float")
        )
        phase_df = phase_df.withColumn("soc", F.col("soc").cast("float"))

        phase_df = phase_df.withColumn(
            "soh",
            F.when(
                (F.col("net_capacity").isNotNull())
                & (F.col("battery_energy").isNotNull())
                & (F.col("soc").between(30, 99))
                & (F.col("IS_USABLE_PHASE") == F.lit(1)),
                F.try_divide(
                    F.col("battery_energy") * 100,
                    F.col("net_capacity") * F.col("soc"),
                ),
            ).otherwise(None),
        )

        if apply_soh_correction:
            phase_df = self._correct_soh_with_temperature(phase_df)

        return phase_df

    def aggregate_stats(self, phase_df):
        agg_columns = [
            # Minimum
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            F.first("range", ignorenulls=True).alias("RANGE"),
            # Renault specific features / Might be able to handle with config
            F.expr("percentile_approx(soh, 0.5)").alias("SOH"),
            F.mean("charging_rate").alias("CHARGING_RATE"),
        ]

        if "consumption" in phase_df.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_aggregated = phase_df.groupBy(
            "VIN",
            "PHASE_INDEX",
            "DATETIME_BEGIN",
            "DATETIME_END",
            "PHASE_STATUS",
            "SOC_FIRST",
            "SOC_LAST",
            "SOC_DIFF",
            "NO_SOC_DATAPOINT",
            "IS_USABLE_PHASE",
        ).agg(*agg_columns)

        return df_aggregated

    def _correct_soh_with_temperature(self, phase_df):
        # Filter to only rows with valid outside_temperature and model
        valid_temp_df = phase_df.filter(
            F.col("outside_temperature").isNotNull() & F.col("model").isNotNull()
        )

        valid_temp_df = valid_temp_df.withColumn(
            "outside_temperature_corrected", F.col("outside_temperature") - 14.0
        )
        assembler = VectorAssembler(
            inputCols=["outside_temperature_corrected"], outputCol="features"
        )
        valid_temp_df = assembler.transform(valid_temp_df)

        car_models = [
            row["model"] for row in valid_temp_df.select("model").distinct().collect()
        ]

        # If no valid car models, return original dataframe unchanged
        if not car_models:
            return phase_df

        renault_soh_model = RenaultSOHModel(spark=self.spark)
        renault_soh_model.load_all()

        pred_cols = []

        for car_model_name in car_models:
            if renault_soh_model.ml_models.get(car_model_name) is None:
                raise ValueError(
                    f"No ML Model found for {renault_soh_model.make} and {car_model_name}"
                )

        valid_temp_df.cache()
        for car_model_name in car_models:
            temp_col = f"soh_pred_{car_model_name}"
            pred_cols.append(temp_col)
            df_model = valid_temp_df.filter(F.col("model") == car_model_name)
            df_model_pred = (
                renault_soh_model.ml_models[car_model_name]
                .transform(df_model)
                .withColumnRenamed("pred_soh", temp_col)
            )
            valid_temp_df = valid_temp_df.join(
                df_model_pred.select("vin", "date", temp_col),
                on=["vin", "date"],
                how="left",
            )
        expr = F.coalesce(*[F.col(c) for c in pred_cols])
        valid_temp_df = valid_temp_df.withColumn("soh_corrected", expr)

        # Join corrected SoH back to original dataframe
        valid_temp_df = valid_temp_df.select("vin", "date", "soh_corrected")
        phase_df = phase_df.join(valid_temp_df, on=["vin", "date"], how="left")

        # Update soh column: use corrected value if available, otherwise keep original
        phase_df = phase_df.withColumn(
            "soh", F.coalesce(F.col("soh_corrected"), F.col("soh"))
        )
        phase_df = phase_df.drop("soh_corrected")

        return phase_df
