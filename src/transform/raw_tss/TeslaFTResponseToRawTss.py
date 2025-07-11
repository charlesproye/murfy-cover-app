from pyspark.sql.functions import col, explode, expr, udf
from pyspark.sql.types import *
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType,
                               IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)
from transform.raw_tss.ResponseToRawTss import ResponseToRawTss
from typing import Optional
from pyspark.sql import SparkSession
from logging import Logger
import logging
from dotenv import load_dotenv
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from core.console_utils import main_decorator
import sys
from pyspark.sql import DataFrame


class TeslaFTResponseToRawTss(ResponseToRawTss):
    """
    Classe pour traiter les données renvoyées par les API Tesla Fleet Telemetry 
    stockées dans /response sur Scaleway
    """

    def __init__(
        self,
        make: str = "tesla-fleet-telemetry",
        force_update: bool = False,
        writing_mode: Optional[str] = "append",
        spark: SparkSession = None,
        logger: Logger = None,
        **kwargs,
    ):

        super().__init__(
            make=make,
            force_update=force_update,
            writing_mode=writing_mode,
            spark=spark,
            logger=logger,
            **kwargs,
        )

    def parse_data(self, df: DataFrame, optimal_partitions_nb: int) -> DataFrame:
        exploded_df = df.select(
            "vin",
            "timestamp",
            "readable_date",
            "createdAt",
            explode("data").alias("data_item"),
        )

        parsed_df = exploded_df.select(
            "vin",
            "timestamp",
            "readable_date",
            "createdAt",
            col("data_item.key").alias("key"),
            col("data_item.value").alias("value"),
        ).coalesce(optimal_partitions_nb)

        def extract_value_standalone(value_struct):
            if value_struct is None:
                return None

            # Essayer stringValue
            if (
                hasattr(value_struct, "stringValue")
                and value_struct.stringValue is not None
            ):
                return str(value_struct.stringValue)
            # Essayer doubleValue
            elif (
                hasattr(value_struct, "doubleValue")
                and value_struct.doubleValue is not None
            ):
                return str(value_struct.doubleValue)
            # Essayer intValue
            elif (
                hasattr(value_struct, "intValue") and value_struct.intValue is not None
            ):
                return str(value_struct.intValue)
            # Essayer booleanValue
            elif (
                hasattr(value_struct, "booleanValue")
                and value_struct.booleanValue is not None
            ):
                return str(value_struct.booleanValue)
            # Essayer detailedChargeStateValue
            elif (
                hasattr(value_struct, "detailedChargeStateValue")
                and value_struct.detailedChargeStateValue is not None
            ):
                return str(value_struct.detailedChargeStateValue)
            # Essayer les autres types si nécessaire
            elif (
                hasattr(value_struct, "cableTypeValue")
                and value_struct.cableTypeValue is not None
            ):
                return str(value_struct.cableTypeValue)
            elif (
                hasattr(value_struct, "climateKeeperModeValue")
                and value_struct.climateKeeperModeValue is not None
            ):
                return str(value_struct.climateKeeperModeValue)
            elif (
                hasattr(value_struct, "defrostModeValue")
                and value_struct.defrostModeValue is not None
            ):
                return str(value_struct.defrostModeValue)
            elif (
                hasattr(value_struct, "fastChargerValue")
                and value_struct.fastChargerValue is not None
            ):
                return str(value_struct.fastChargerValue)
            elif (
                hasattr(value_struct, "hvacAutoModeValue")
                and value_struct.hvacAutoModeValue is not None
            ):
                return str(value_struct.hvacAutoModeValue)
            elif (
                hasattr(value_struct, "hvacPowerValue")
                and value_struct.hvacPowerValue is not None
            ):
                return str(value_struct.hvacPowerValue)

            return None

        extract_value_udf = udf(extract_value_standalone, StringType())

        parsed_df = parsed_df.select(
            "vin",
            "timestamp",
            "readable_date",
            "createdAt",
            "key",
            extract_value_udf("value").alias("value"),
        )

        pivoted = (
            parsed_df.repartition("vin")
            .groupBy("vin", "timestamp", "readable_date", "createdAt")
            .pivot("key")
            .agg(expr("first(value)"))
            .withColumnRenamed("readable_date", "date")
            .coalesce(optimal_partitions_nb)
        )

        return pivoted


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    logger = logging.getLogger('Logger RawTss')

    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    TeslaFTResponseToRawTss(force_update=True, spark=spark, logger=logger)


if __name__ == "__main__":
    main()


