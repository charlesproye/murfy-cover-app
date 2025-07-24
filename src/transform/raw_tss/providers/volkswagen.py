from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import first

from transform.raw_tss.response_to_raw import ResponseToRawTss


class VolkswagenResponseToRaw(ResponseToRawTss):
    """
    Class for processing data emitted by BMW APIs
    stored in '/response/bmw/' on Scaleway
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
        Parse dict from Volkswagen API response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): active Spark session
            vin (str): Vehicle Identification Number

        Returns:
            spark.DataFrame: Data with all columns
        """

        parsed = df.selectExpr("explode(data) as row").select("row.*")
        parsed = parsed.withColumnRenamed("received_date", "date")
        columns_to_aggregate = [
            col for col in parsed.columns if col not in ("vin", "date")
        ]
        aggregations = [
            first(col, ignorenulls=True).alias(col) for col in columns_to_aggregate
        ]

        grouped = parsed.groupBy("vin", "date").agg(*aggregations)

        return grouped

