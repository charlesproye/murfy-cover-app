from abc import abstractmethod
from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from core.caching_utils import CachedETLSpark
from transform.processed_results.config import UPDATE_FREQUENCY
from transform.processed_tss.main import ORCHESTRATED_MAKES
from transform.raw_results.config import RAW_RESULTS_CACHE_KEY_TEMPLATE

# TODO
# Voir s'il est pertinent de mettre en place une optimisation de la taille des partitions pour cette étape


class ProcessedTsToRawResults(CachedETLSpark):
    """
    Class to transform processed time series data into raw results

    Default class works for makes without SOH computation
    """

    def __init__(
        self,
        make,
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs,
    ):
        self.make = make
        self.spark = spark
        self.logger = logger
        super().__init__(
            RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make=make.replace("-", "_")),
            "s3",
            force_update=force_update,
            spark=spark,
            **kwargs,
        )

    def run(self):

        self.logger.info(f"Traitement débuté pour {self.make}")

        cls = ORCHESTRATED_MAKES[self.make][1]
        pts = cls(make=self.make, spark=self.spark, logger=self.logger).data

        df = self.aggregate(pts)

        df = self.compute_specific_features(pts, df)

        df = self.compute_consumption(df)

        df = self.compute_soh(df)
        df = self.compute_cycles(df)

        df = self.compute_charge_levels(df)

        return df

    def aggregate(self, pts: DataFrame):
        """
        Aggregate the processed time series data by vin and date to get the raw results

        Default case when there is no SOH computation implemented
        """

        floored_col = F.date_trunc("week", F.col("date")).alias("floored_date")

        agg_columns = [
            F.last("odometer", ignorenulls=True).alias("odometer"),
            F.first("make", ignorenulls=True).alias("make"),
            F.first("model", ignorenulls=True).alias("model"),
            F.first("version", ignorenulls=True).alias("version"),
        ]

        if "consumption" in pts.columns:
            agg_columns.append(F.mean("consumption").alias("consumption"))
        else:
            agg_columns.append(F.lit(None).cast(T.FloatType()).alias("consumption"))

        df = (
            pts.withColumn("floored_date", floored_col)
            .groupBy("vin", "floored_date")
            .agg(*agg_columns)
            .withColumnRenamed("floored_date", "date")
            .na.drop(subset=["odometer"])
            .withColumn("soh", F.lit(None).cast(T.FloatType()))
        )

        return df

    def compute_specific_features(self, pts: DataFrame, df: DataFrame):
        return df

    def compute_soh(self, df: DataFrame):
        return df

    def compute_consumption(self, df: DataFrame):
        return df

    def compute_charge_levels(self, df: DataFrame):
        return df

    def compute_cycles(self, df: DataFrame):
        return df

