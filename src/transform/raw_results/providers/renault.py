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

        pts = (
            pts.withColumn(
                "expected_battery_energy",
                F.when(
                    (F.col("net_capacity").isNotNull()) & (F.col("soc").isNotNull()),
                    F.col("net_capacity") * (F.col("soc") / 100.0)
                )
                .otherwise(None)
            )
        )

        df = (
            pts
            .groupBy(['vin', 'charging_status_idx'])
            .agg(
                F.first('charging_status', ignorenulls=True).alias('charging_status'),
                F.sum('battery_energy').alias('battery_energy_sum'),
                F.sum('expected_battery_energy').alias('expected_battery_energy_sum'),
            )
            .withColumn(
                'expected_battery_energy_sum',
                F.when(F.col('charging_status') == 'discharging', None).otherwise(F.col('expected_battery_energy_sum'))
            )
        )

        return df

    def compute_specific_features(self, pts, df):
        df_soc_diff = pts.groupBy(['vin', 'charging_status_idx']).agg(
            F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
            F.first("odometer", ignorenulls=True).alias("odometer"),
            F.first("version", ignorenulls=True).alias("version"),
            F.first('soc', ignorenulls=True).alias('soc_first'),
            F.last('soc', ignorenulls=True).alias('soc_last'),
            F.first("model", ignorenulls=True).alias("model"),
            F.first("date", ignorenulls=True).alias("date"),
            F.last("range", ignorenulls=True).alias("range"),
            F.first("odometer", ignorenulls=True).alias("odometer_start"),
            F.last("odometer", ignorenulls=True).alias("odometer_end"),
            F.mean("charging_rate").alias("charging_rate")
        )

        df_soc_diff = df_soc_diff.withColumn("soc_diff", F.col("soc_last") - F.col("soc_first"))
        df_soc_diff = df_soc_diff.withColumn("odometer_diff", F.col("odometer_end") - F.col("odometer_start"))


        df = df.join(df_soc_diff, on=['vin', 'charging_status_idx'], how='left')

        return df

    def compute_soh(self, df):

        df = df.withColumn(
            'soh', 
            F.col('battery_energy_sum') / F.col('expected_battery_energy_sum')
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
            df.withColumn(
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

