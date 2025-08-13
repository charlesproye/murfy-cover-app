from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, expr, udf
from pyspark.sql.types import *

from transform.raw_tss.response_to_raw import ResponseToRawTss


class TeslaFTResponseToRawTss(ResponseToRawTss):
    """
    Class to process data returned by the Tesla Fleet Telemetry APIs
    stored in /response on Scaleway
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

            # Try stringValue
            if (
                hasattr(value_struct, "stringValue")
                and value_struct.stringValue is not None
            ):
                return str(value_struct.stringValue)
            # Try doubleValue
            elif (
                hasattr(value_struct, "doubleValue")
                and value_struct.doubleValue is not None
            ):
                return str(value_struct.doubleValue)
            # Try intValue
            elif (
                hasattr(value_struct, "intValue") and value_struct.intValue is not None
            ):
                return str(value_struct.intValue)
            # Try booleanValue
            elif (
                hasattr(value_struct, "booleanValue")
                and value_struct.booleanValue is not None
            ):
                return str(value_struct.booleanValue)
            # Try detailedChargeStateValue
            elif (
                hasattr(value_struct, "detailedChargeStateValue")
                and value_struct.detailedChargeStateValue is not None
            ):
                return str(value_struct.detailedChargeStateValue)
            # Try other types if necessary
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

    def _get_dynamic_schema(self, field_def: dict, parse_type_map: dict):
        return StructType(
                [
                    StructField("vin", StringType(), True),
                    StructField("timestamp", LongType(), True),
                    StructField("readable_date", StringType(), True),
                    StructField("createdAt", StringType(), True),
                    StructField(
                        "data",
                        ArrayType(
                            StructType(
                                [
                                    StructField("key", StringType(), True),
                                    StructField(
                                        "value",
                                        StructType(
                                            [
                                                StructField("doubleValue", DoubleType(), True),
                                                StructField("intValue", IntegerType(), True),
                                                StructField(
                                                    "booleanValue", BooleanType(), True
                                                ),
                                                StructField("stringValue", StringType(), True),
                                                StructField("carTypeValue", StringType(), True),
                                                StructField(
                                                    "bmsStateValue", StringType(), True
                                                ),
                                                StructField(
                                                    "climateKeeperModeValue", StringType(), True
                                                ),
                                                StructField(
                                                    "chargePortValue", StringType(), True
                                                ),
                                                StructField(
                                                    "defrostModeValue", StringType(), True
                                                ),
                                                StructField(
                                                    "detailedChargeStateValue",
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    "fastChargerValue", StringType(), True
                                                ),
                                                StructField(
                                                    "hvacAutoModeValue", StringType(), True
                                                ),
                                                StructField(
                                                    "hvacPowerValue", StringType(), True
                                                ),
                                                StructField(
                                                    "sentryModeStateValue", StringType(), True
                                                ),
                                                StructField("invalid", BooleanType(), True),
                                            ]
                                        ),
                                    ),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            )
