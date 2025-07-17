from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, first, to_timestamp
from pyspark.sql.types import *

from transform.raw_tss.base.response_to_raw import ResponseToRawTss


class BMWResponseToRaw(ResponseToRawTss):
    """
    Classe pour traiter les données émises par les API BMW
    stockées dans '/response/bmw/' sur Scaleway
    """

    def __init__(
        self,
        make: str = "bmw",
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
        """
        Parse dict from BMW api response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): spark session active
            vin (str): Vehicle identification number

        Returns:
            spark.DataFrame: Data with every columns
        """

        df = df.coalesce(optimal_partitions_nb)

        df_exploded = df.selectExpr("explode(data) as record")

        df_flat = df_exploded.select(
            col("record.vin").alias("vin"),
            explode(col("record.pushKeyValues")).alias("kv"),
        )

        df_parsed = df_flat.select(
            col("vin"),
            col("kv.key").alias("key"),
            col("kv.value").alias("value"),
            col("kv.date_of_value").alias("date"),
        )

        df_parsed = df_parsed.withColumn("date", to_timestamp("date"))
        pivoted = (
            df_parsed.repartition("vin")
            .groupBy("vin", "date")
            .pivot("key")
            .agg(first("value"))
            .coalesce(optimal_partitions_nb)
        )

        return pivoted

