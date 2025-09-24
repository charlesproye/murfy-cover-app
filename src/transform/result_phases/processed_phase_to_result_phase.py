from logging import getLogger
from dotenv import load_dotenv

from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.caching_utils import CachedETLSpark
from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings

from transform.result_phases.config import (
    RESULT_PHASES_CACHE_KEY_TEMPLATE
)

from transform.processed_phases.main import *

load_dotenv() 


class ProcessedPhaseToResultPhase(CachedETLSpark):

    def __init__(
        self,
        make: str,
        id_col: str = "vin",
        log_level: str = "INFO",
        force_update: bool = False,
        spark: SparkSession = None,
        **kwargs,
    ):
        self.make = make
        logger_name = f"transform.result_phase.{make}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.spark = spark
        self.bucket = S3Service()
        self.settings = S3Settings()

        super().__init__(
            RESULT_PHASES_CACHE_KEY_TEMPLATE.format(make=make.replace("-", "_")),
            "s3",
            force_update=force_update,
            spark=spark,
            repartition_key='VIN',
            **kwargs,
        )

    def run(self):
        # Load the raw tss dataframe
        self.logger.info(f"Running for {self.make}")

        cls = ORCHESTRATED_MAKES[self.make][1]   

        pph = cls(make=self.make, spark=self.spark, logger=self.logger).data

        pph = self.compute_specific_features(pph)
        pph = self.compute_soh(pph)
        pph = self.compute_consumption(pph)
        pph = self.compute_charge_levels(pph)

        return pph

    def compute_specific_features(self, pph: DF) -> DF:
        """
        Compute the specific features
        """
        return pph

    def compute_soh(self, df_aggregated: DF) -> DF:
        """
        Compute the soh
        """
        return df_aggregated


    def compute_charge_levels(self, df_aggregated: DF) -> DF:
        """
        Compute the charge levels
        """
        return df_aggregated

    
    def compute_consumption(self, phase_df: DF) -> DF:
        """
        Compute the consumption
        """

        if "CONSUMPTION" not in phase_df.columns:
            phase_df = (
                phase_df
                .withColumn("ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST"))
                .withColumn(
                    "CONSUMPTION",
                    F.when(F.col("PHASE_STATUS") == "discharging",
                    (- 1 * F.col("SOC_DIFF"))
                    * (F.col("BATTERY_NET_CAPACITY"))
                    / F.col("ODOMETER_DIFF"),
                    ).otherwise(None)
                )
            )

        return phase_df

