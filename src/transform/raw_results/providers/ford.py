from transform.raw_results.processed_ts_to_raw_results import ProcessedTsToRawResults
from pyspark.sql import SparkSession
from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from core.stats_utils import estimate_cycles

class FordProcessedTsToRawResults(ProcessedTsToRawResults):

    def __init__(self, make='ford', spark: SparkSession=None, force_update:bool=False, logger: Logger=None, **kwargs):
        super().__init__(make, spark=spark, force_update=force_update, logger=logger, **kwargs)

    def aggregate(self, pts: DataFrame):
        return pts


    def compute_specific_features(self, df):
        """
        Compute the max battery energy for each net_capacity and soc
        """

        max_energy = (
            df
            .groupBy("net_capacity", "soc")
            .agg(F.expr("percentile_approx(battery_energy, 0.9)").alias("max_battery_energy"))
        )

        df = (
            df
            .join(max_energy, on=["net_capacity", "soc"], how="left")
        )

        return df
    

    def compute_soh(self, df):
        """
        Compute the State of Health for Ford
        """

        df = df.withColumn(
            "soh", 
            F.when(F.col("max_battery_energy").isNotNull() & (F.col("max_battery_energy") != 0), 
                F.col("battery_energy") / F.col("max_battery_energy"))
            .otherwise(None)
        )

        return df
    
    def compute_consumption(self, df):
        """
        Compute the consumption for Ford
        """

        df_filtered = df.dropna(subset=['odometer', 'soc'])

        consumption = (
            df_filtered
            .filter(F.col("charging_status") == "discharging")
            .groupBy("vin", "charging_status_idx")
            .agg(
                F.first("soc").alias("soc_start"),
                F.last("soc").alias("soc_end"),
                F.first("odometer").alias("odometer_start"),
                F.last("odometer").alias("odometer_end"),
                F.first("net_capacity").alias("net_capacity")
            )
            .withColumn("soc_diff", F.col("soc_start") - F.col("soc_end"))
            .withColumn("odometer_diff", F.col("odometer_end") - F.col("odometer_start"))
            .withColumn("consumption", F.col("soc_diff") * F.col("net_capacity") * 100 / F.col("odometer_diff"))
            .filter(F.col("consumption") > 0)
            .filter(F.col("odometer_diff") > 10)
            .select("vin", "charging_status_idx", "consumption")
        )

        return df.join(consumption, on=["vin", "charging_status_idx"], how="left")
    

    
    def compute_cycles(self, df: DataFrame):
        """
        Compute the cycles for Ford
        """

        def estimate_cycles_udf(odometer, range_val, soh):
            return estimate_cycles(odometer, range_val, soh)

        estimate_cycles_spark = udf(estimate_cycles_udf, DoubleType())

        df  = df.withColumn("cycles", estimate_cycles_spark(F.col("odometer"), F.col("range"), F.col("soh")))

        return df
