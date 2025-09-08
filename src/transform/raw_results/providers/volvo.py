from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from core.stats_utils import estimate_cycles
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults


class VolvoProcessedTsToRawResults(ProcessedTsToRawResults):

    def __init__(
        self,
        make="volvo-cars",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def aggregate(self, pts: DataFrame):

        w = Window.partitionBy("vin").orderBy("date")

        pts = pts.withColumn(
            "odometer",
            F.coalesce(
                F.last("odometer", ignorenulls=True).over(w),
                F.first("odometer", ignorenulls=True).over(
                    w.orderBy(F.col("date").desc())
                ),
            ),
        ).withColumn(
            "soh",
            F.when(
                (F.col("soc").isNotNull())
                & (F.col("range").isNotNull())
                & (F.col("range") != 0),
                F.col("estimated_range")
                / (F.col("soc") / 100.0)
                / F.col("range")
                / F.lit(0.87),
            ).otherwise(None),
        )

        df = (
            pts
            .groupBy(['vin', 'charging_status_idx'])
            .agg(
                F.expr("percentile_approx(soh, 0.5)").alias('soh'),
                F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
                F.first("odometer", ignorenulls=True).alias("odometer"),
                F.first("version", ignorenulls=True).alias("version"),
                F.first('soc', ignorenulls=True).alias('soc_first'),
                F.last('soc', ignorenulls=True).alias('soc_last'),
                F.first("model", ignorenulls=True).alias("model"),
                F.first("date", ignorenulls=True).alias("date"),
                F.first('range', ignorenulls=True).alias('range'),
                F.mean('consumption').alias('consumption'),
                F.first('charging_status', ignorenulls=True).alias('charging_status')
            )
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


    def compute_consumption(self, df):
        return df
