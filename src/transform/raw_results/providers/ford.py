from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

from core.stats_utils import estimate_cycles
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults


class FordProcessedTsToRawResults(ProcessedTsToRawResults):

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

    def aggregate(self, pts: DataFrame):

        pts = pts.withColumn('battery_energy', F.col('battery_energy').cast('double'))
        pts_charging = pts.filter(F.col("charging_status") == "charging")

        max_energy = pts_charging.groupBy(["net_capacity", "soc"]).agg(
            F.expr("percentile_approx(battery_energy, 0.9)").alias("max_battery_energy")
        )

        df = pts.join(max_energy, on=["net_capacity", "soc"], how="left")

        df = df.withColumn('max_battery_energy', F.col('max_battery_energy').cast('double'))

        df = (
            df
            .dropna(subset=['battery_energy', 'max_battery_energy'])  
            .groupBy(['vin', 'charging_status_idx'])
            .agg(
                F.first('charging_status', ignorenulls=True).alias('charging_status'),
                F.sum('battery_energy').alias('battery_energy_sum'),
                F.sum('max_battery_energy').alias('max_battery_energy_sum'),
            )
            .withColumn(
                'battery_energy_sum',
                F.when(F.col('charging_status') == 'discharging', None).otherwise(F.col('battery_energy_sum'))
            )
            .withColumn(
                'max_battery_energy_sum',
                F.when(F.col('charging_status') == 'discharging', None).otherwise(F.col('max_battery_energy_sum'))
            )
        )

        return df

    def compute_specific_features(self, pts, df):
        df_soc_diff = pts.groupBy(['vin', 'charging_status_idx']).agg(
            F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
            F.first("odometer", ignorenulls=True).alias("odometer"),
            F.first("version", ignorenulls=True).alias("version"),
            F.first("model", ignorenulls=True).alias("model"),
            F.last('soc', ignorenulls=True).alias('soc_last'),
            F.first('soc', ignorenulls=True).alias('soc_first'),
            F.first('date', ignorenulls=True).alias('date'),
            F.first("odometer", ignorenulls=True).alias("odometer_start"),
            F.last("odometer", ignorenulls=True).alias("odometer_end"),
            F.last("range", ignorenulls=True).alias("range"),
        )

        df_soc_diff = df_soc_diff.withColumn("soc_diff", F.col("soc_last") - F.col("soc_first"))
        df_soc_diff = df_soc_diff.withColumn("odometer_diff", F.col("odometer_end") - F.col("odometer_start"))

        df = df.join(df_soc_diff, on=['vin', 'charging_status_idx'], how='left')

        return df

    def compute_soh(self, df):
        """
        Compute the State of Health for Ford
        """

        df = df.withColumn(
            'soh', 
            F.col('battery_energy_sum') / F.col('max_battery_energy_sum')
        )

        return df


    def compute_cycles(self, df: DataFrame):
        """
        Compute the cycles for Ford
        """

        def estimate_cycles_udf(odometer, range_val, soh):
            return estimate_cycles(odometer, range_val, soh)

        estimate_cycles_spark = udf(estimate_cycles_udf, DoubleType())

        df = df.withColumn(
            "cycles",
            estimate_cycles_spark(F.col("odometer"), F.col("range"), F.col("soh")),
        )

        return df

