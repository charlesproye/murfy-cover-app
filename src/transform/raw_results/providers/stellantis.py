from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from core.stats_utils import estimate_cycles
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults


class StellantisProcessedTsToRawResults(ProcessedTsToRawResults):

    def __init__(
        self,
        make="stellantis",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(make, spark, force_update, logger=logger, **kwargs)

    def aggregate(self, pts: DataFrame):

        df = (
            pts
            .groupBy(['vin', 'charging_status_idx'])
            .agg(
                F.expr("percentile_approx(soh_oem, 0.5)").alias('soh_oem'),
                F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
                F.first("odometer", ignorenulls=True).alias("odometer"),
                F.first("version", ignorenulls=True).alias("version"),
                F.first('soc', ignorenulls=True).alias('soc_first'),
                F.last('soc', ignorenulls=True).alias('soc_last'),
                F.first("model", ignorenulls=True).alias("model"),
                F.first("date", ignorenulls=True).alias("date"),
                F.first('charging_status', ignorenulls=True).alias('charging_status'),
                F.first("odometer", ignorenulls=True).alias("odometer_start"),
                F.last("odometer", ignorenulls=True).alias("odometer_end")
            )
        )

        return df

    
    def compute_consumption(self, df):
        """
        Compute the consumption for Stellantis
        """

        df = df.withColumn("odometer_diff", F.col("odometer_end") - F.col("odometer_start"))
        df = df.withColumn("soc_diff", (F.col("soc_last") - F.col("soc_first")) / 100)

        consumption = (
            df.filter(F.col("charging_status") == "discharging")
            .withColumn(
                "consumption",
                F.when(
                    F.col("soh_oem").isNotNull(),
                    (- 1 * F.col("soc_diff"))
                    * (F.col("net_capacity")) * (F.col("soh_oem") / 100)
                    / F.col("odometer_diff")
                ).otherwise(
                    (- 1 * F.col("soc_diff"))
                    * (F.col("net_capacity"))
                    / F.col("odometer_diff")
                )
            )
            .filter(F.col("consumption") > 0)
            .filter(F.col("odometer_diff") > 10)
            .select("vin", "charging_status_idx", "consumption")
        )

        return df.join(consumption, on=["vin", "charging_status_idx"], how="left")
