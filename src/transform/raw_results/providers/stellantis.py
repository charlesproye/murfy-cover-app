from transform.raw_results.processed_ts_to_raw_results import ProcessedTsToRawResults
from pyspark.sql import SparkSession
from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from core.stats_utils import estimate_cycles

class StellantisProcessedTsToRawResults(ProcessedTsToRawResults):

    def __init__(self, make='stellantis', spark: SparkSession=None, force_update:bool=False, logger: Logger=None, **kwargs):
        super().__init__(make, spark, force_update, logger=logger, **kwargs)

    def aggregate(self, pts: DataFrame):
        return pts


    def compute_cycles(self, df: DataFrame):
        """
        Compute the cycles for Ford
        """

        def estimate_cycles_udf(odometer, range_val, soh):
            return estimate_cycles(odometer, range_val, soh)

        estimate_cycles_spark = udf(estimate_cycles_udf, DoubleType())

        df  = df.withColumn("cycles", estimate_cycles_spark(F.col("odometer"), F.col("range"), F.col("soh")))

        return df
