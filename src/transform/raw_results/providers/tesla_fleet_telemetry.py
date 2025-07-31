from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transform.raw_results.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults


class TeslaFTProcessedTsToRawResults(ProcessedTsToRawResults):

    def __init__(
        self,
        make="tesla-fleet-telemetry",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs,
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def aggregate(self, pts: DataFrame):
        """
        Aggregate the processed time series data by vin and charging_status_idx to get the raw results
        """

        df = pts.groupBy("vin", "charging_status_idx").agg(
            F.min("ac_charge_energy_added").alias("ac_energy_added_min"),
            F.min("dc_charge_energy_added").alias("dc_energy_added_min"),
            F.last("ac_charge_energy_added", ignorenulls=True).alias(
                "ac_energy_added_end"
            ),
            F.last("dc_charge_energy_added", ignorenulls=True).alias(
                "dc_energy_added_end"
            ),
            F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
            F.first("range", ignorenulls=True).alias("range"),
            F.first("odometer", ignorenulls=True).alias("odometer"),
            F.first("version", ignorenulls=True).alias("version"),
            F.count("soc").alias("size"),
            F.first("model", ignorenulls=True).alias("model"),
            F.first("date", ignorenulls=True).alias("date"),
            F.expr("percentile_approx(ac_charging_power, 0.5)").alias(
                "ac_charging_power"
            ),
            F.expr("percentile_approx(dc_charging_power, 0.5)").alias(
                "dc_charging_power"
            ),
            F.first("tesla_code", ignorenulls=True).alias("tesla_code"),
        )

        return df

    def compute_specific_features(self, pts, df):
        """
        Compute specific features for tesla fleet telemetry : soc diff, charging power, energy added
        """

        window_spec = Window.partitionBy("vin", "charging_status_idx").orderBy("date")

        soc_diff_df = (
            pts.groupBy("vin", "charging_status_idx")
            .agg(
                F.first(
                    F.when(F.col("soc").isNotNull(), F.col("soc")), ignorenulls=True
                ).alias("soc_start"),
                F.last(
                    F.when(F.col("soc").isNotNull(), F.col("soc")), ignorenulls=True
                ).alias("soc_end"),
            )
            .withColumn("soc_diff", F.col("soc_end") - F.col("soc_start"))
        )

        df = df.join(soc_diff_df, on=["vin", "charging_status_idx"], how="left")

        df = df.withColumn(
            "charging_power",
            F.coalesce(F.col("ac_charging_power"), F.lit(0))
            + F.coalesce(F.col("dc_charging_power"), F.lit(0)),
        )

        df = (
            df.withColumn(
                "ac_energy_added",
                F.col("ac_energy_added_end") - F.col("ac_energy_added_min"),
            )
            .withColumn(
                "dc_energy_added",
                F.col("dc_energy_added_end") - F.col("dc_energy_added_min"),
            )
            .withColumn("energy_added", F.col("dc_energy_added"))
        )

        return df

    def compute_soh(self, df):
        """
        Compute the State of Health for tesla fleet telemetry
        """
        required_col = {"energy_added", "soc_diff", "net_capacity"}
        missing_col = required_col - set(df.columns)
        assert not missing_col, f"Missing columns in DataFrame: {missing_col}"
        return df.withColumn(
            "soh",
            F.col("energy_added") / (F.col("soc_diff") / 100.0 * F.col("net_capacity")),
        )

    def compute_charge_levels(self, df):
        """
        Compute the charge levels for tesla fleet telemetry
        """

        df = (
            df.withColumn(
                "level_1",
                F.col("soc_diff")
                * F.when(F.col("charging_power") < LEVEL_1_MAX_POWER, 1).otherwise(0)
                / 100,
            )
            .withColumn(
                "level_2",
                F.col("soc_diff")
                * F.when(
                    F.col("charging_power").between(
                        LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
                    ),
                    1,
                ).otherwise(0)
                / 100,
            )
            .withColumn(
                "level_3",
                F.col("soc_diff")
                * F.when(F.col("charging_power") > LEVEL_2_MAX_POWER, 1).otherwise(0)
                / 100,
            )
        )

        return df

    def compute_cycles(self, df, initial_range: float = 1, default_soh: float = 1.0):
        """
        Estimate the number of cycles for tesla fleet telemetry
        """
        return df.withColumn(
            "estimated_cycles",
            F.round(
                F.col("odometer")
                / (
                    (
                        F.when(F.col("range").isNull(), F.lit(initial_range)).otherwise(
                            F.col("range")
                        )
                    )
                    * (
                        (
                            F.when(
                                F.col("soh").isNull() | F.isnan(F.col("soh")),
                                F.lit(default_soh),
                            ).otherwise(F.col("soh"))
                        )
                        + F.lit(1)
                    )
                    / F.lit(2)
                )
            ),
        )

