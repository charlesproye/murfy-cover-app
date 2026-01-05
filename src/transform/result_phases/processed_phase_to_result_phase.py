from logging import getLogger

from dagster_pipes import PipesContext
from dotenv import load_dotenv
from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.caching_utils import CachedETLSpark
from core.logging_utils import set_level_of_loggers_with_prefix
from core.models.make import MakeEnum
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from transform.processed_phases.main import PROVIDERS
from transform.result_phases.config import RESULT_PHASES_CACHE_KEY_TEMPLATE

load_dotenv()


class ProcessedPhaseToResultPhase(CachedETLSpark):
    def __init__(
        self,
        make: str | MakeEnum,
        id_col: str = "vin",
        log_level: str = "INFO",
        force_update: bool = False,
        spark: SparkSession = None,  # type: ignore[assignment]
        pipes: PipesContext = None,  # type: ignore[assignment]
        **kwargs,
    ):
        # Normalize make to MakeEnum if it's a string
        if isinstance(make, str):
            self.make = MakeEnum(make)
        else:
            self.make = make
        logger_name = f"transform.result_phase.{self.make.value}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.spark = spark
        self.bucket = S3Service()
        self.settings = S3Settings()
        self.pipes = pipes
        self.force_update = force_update

        super().__init__(
            "s3",
            bucket=self.bucket,
            settings=self.settings,
            spark=spark,
            repartition_key="VIN",
            **kwargs,
        )

    def run(self):
        # Load the raw tss dataframe
        self.pipes.log.info(f"Running for {self.make}")

        if self.bucket.check_spark_file_exists(
            f"processed_phases/processed_phases_{self.make.value.replace('-', '_')}.parquet"
        ):
            pph = self.bucket.read_parquet_df_spark(
                spark=self.spark,
                key=f"processed_phases/processed_phases_{self.make.value.replace('-', '_')}.parquet",
            )
        else:
            pph = PROVIDERS[self.make](
                self.make,
                spark=self.spark,
                force_update=self.force_update,
                pipes=self.pipes,
            ).run()

        pph = self.compute_specific_features(pph)
        pph = self.compute_soh(pph)
        pph = self.compute_odometer_diff(pph)
        pph = self.compute_consumption(pph)
        pph = self.compute_ratio_km_soc(pph)
        pph = self.compute_charge_levels(pph)

        self.save(
            pph,
            RESULT_PHASES_CACHE_KEY_TEMPLATE.format(
                make=self.make.value.replace("-", "_")
            ),
            force_update=self.force_update,
        )

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

    def compute_odometer_diff(self, phase_df: DF) -> DF:
        """
        Compute the odometer difference
        """
        return phase_df.withColumn(
            "ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST")
        )

    def compute_consumption(self, phase_df: DF) -> DF:
        """
        Compute the consumption
        """

        if "CONSUMPTION" not in phase_df.columns:
            phase_df = phase_df.withColumn(
                "ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST")
            ).withColumn(
                "CONSUMPTION",
                F.when(
                    (F.col("PHASE_STATUS") == "discharging")
                    & (F.col("ODOMETER_DIFF") > 5),
                    (-1 * F.col("SOC_DIFF"))
                    * (F.col("BATTERY_NET_CAPACITY"))
                    / F.col("ODOMETER_DIFF"),
                ).otherwise(None),
            )

        return phase_df

    def compute_ratio_km_soc(self, pph):
        # Add ODOMETER_DIFF if necessary
        if "ODOMETER_DIFF" not in pph.columns:
            pph = pph.withColumn(
                "ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST")
            )

        # filter dataframe to keep only valid data
        pph_clean = pph.dropna(subset=["ODOMETER_DIFF", "SOC_DIFF"])
        pph_filtered = pph_clean.filter(
            (F.col("SOC_DIFF") < 0) & (F.col("ODOMETER_DIFF") > 0)
        )
        # calculate ratios km/soc
        pph_with_ratios = pph_filtered.withColumn(
            "RATIO_KM_SOC", F.col("ODOMETER_DIFF") / F.abs(F.col("SOC_DIFF"))
        )

        pph = pph.join(
            pph_with_ratios.select("VIN", "RATIO_KM_SOC", "PHASE_INDEX"),
            ["VIN", "PHASE_INDEX"],
            "left",
        )
        return pph
