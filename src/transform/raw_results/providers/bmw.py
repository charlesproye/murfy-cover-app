from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

from core.stats_utils import estimate_cycles
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults


class BMWProcessedTsToRawResults(ProcessedTsToRawResults):

    def __init__(
        self,
        make="bmw",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )

    def compute_consumption(self, df):
        """
        Compute the consumption for BMW
        Already availble so avoid the defautl behavior to compute consumption
        """
        return df

