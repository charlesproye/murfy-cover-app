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

        df = self.compute_soh(df)
        df = self.compute_consumption(df)
        df = self.compute_cycles(df)

        df = self.compute_charge_levels(df)

        return df

    def aggregate(self, pts: DataFrame):
        """
        Aggregate the processed time series data by vin and date to get the raw results

        Default case when there is no SOH computation implemented
        """

        agg_columns = [
            F.last("odometer", ignorenulls=True).alias("odometer"),
            F.first("make", ignorenulls=True).alias("make"),
            F.first("model", ignorenulls=True).alias("model"),
            F.first("version", ignorenulls=True).alias("version"),
            F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
            F.first("date", ignorenulls=True).alias("date"),
            F.first("soc", ignorenulls=True).alias("soc_first"),
            F.last("soc", ignorenulls=True).alias("soc_last"),
            F.first("charging_status", ignorenulls=True).alias("charging_status"),
            F.first("odometer", ignorenulls=True).alias("odometer_start"),
            F.last("odometer", ignorenulls=True).alias("odometer_end")
        ]

        if "consumption" in pts.columns:
            agg_columns.append(F.mean("consumption").alias("consumption"))

        df = (
            pts.groupBy("vin", "charging_status_idx")
            .agg(*agg_columns)
            .na.drop(subset=["odometer"])
        )

        return df

    def compute_specific_features(self, pts: DataFrame, df: DataFrame):

        df = df.withColumn("odometer_diff", F.col("odometer_end") - F.col("odometer_start"))
        df = df.withColumn("soc_diff", F.col("soc_last") - F.col("soc_first"))


        return df

    def compute_soh(self, df: DataFrame):
        return df

    def compute_consumption(self, df: DataFrame):

        consumption = (
            df.filter(F.col("charging_status") == "discharging")
            .withColumn(
                "consumption",
                (- 1 * F.col("soc_diff"))
                * (F.col("net_capacity"))
                / F.col("odometer_diff"),
            )
            .filter(F.col("consumption") > 0)
            .filter(F.col("odometer_diff") > 10)
            .select("vin", "charging_status_idx", "consumption")
        )

        return df.join(consumption, on=["vin", "charging_status_idx"], how="left")

    def compute_charge_levels(self, df: DataFrame):
        return df

    def compute_cycles(self, df: DataFrame):
        return df

