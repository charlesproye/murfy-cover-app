from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from core.stats_utils import estimate_cycles
from transform.raw_results.config import LEVEL_1_MAX_POWER, LEVEL_2_MAX_POWER
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults


class RenaultProcessedTsToRawResults(ProcessedTsToRawResults):

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

    def aggregate(self, pts: DataFrame):
        return pts

    def compute_specific_features(self, df):
        df = df.filter(F.col("charging_status") == "charging").withColumn(
            "expected_battery_energy",
            F.when(
                F.col("net_capacity").isNotNull() & F.col("soc").isNotNull(),
                F.col("net_capacity") * (F.col("soc") / 100.0),
            ).otherwise(None),
        )

        return df

    def compute_soh(self, df):
        df = df.withColumn(
            "soh",
            F.when(
                F.col("expected_battery_energy").isNotNull()
                & (F.col("expected_battery_energy") != 0),
                F.col("battery_energy") / F.col("expected_battery_energy"),
            ).otherwise(None),
        )

        return df

    def compute_cycles(self, df):
        def estimate_cycles_udf(odometer, range_val, soh):
            return estimate_cycles(odometer, range_val, soh)

        estimate_cycles_spark = udf(estimate_cycles_udf, DoubleType())

        df = df.withColumn(
            "cycles",
            estimate_cycles_spark(F.col("odometer"), F.col("range"), F.col("soh")),
        )

        return df

    def compute_charge_levels(self, df):
        w = Window.partitionBy("vin").orderBy("date")

        return (
            df.withColumn("soc_diff", F.col("soc") - F.lag("soc").over(w))
            .withColumn(
                "level_1",
                F.col("soc_diff")
                * (F.col("charging_rate") < F.lit(LEVEL_1_MAX_POWER)).cast("int"),
            )
            .withColumn(
                "level_2",
                F.col("soc_diff")
                * F.col("charging_rate")
                .between(F.lit(LEVEL_1_MAX_POWER), F.lit(LEVEL_2_MAX_POWER))
                .cast("int"),
            )
            .withColumn(
                "level_3",
                F.col("soc_diff")
                * (F.col("charging_rate") > F.lit(LEVEL_2_MAX_POWER)).cast("int"),
            )
        )

