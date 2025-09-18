from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import first
from pyspark.sql.types import *

from transform.raw_tss.response_to_raw import ResponseToRawTss



class TeslaResponseToRaw(ResponseToRawTss):
    """
    Class for processing data emitted by Tesla APIs
    stored in '/response/bmw/' on Scaleway
    """

    def __init__(
        self,
        make: str = "tesla",
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
        Parse dict from Tesla API response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): active Spark session
            vin (str): Vehicle Identification Number

        Returns:
            spark.DataFrame: Data with all columns
        """

        # On renomme readable_date en date
        df = df.coalesce(optimal_partitions_nb)
        
        df = df.withColumnRenamed("readable_date", "date")

        # Colonnes à agréger (tout sauf vin et date)
        cols_to_agg = [c for c in df.columns if c not in ["vin", "date"]]

        # Construit dynamiquement l'agg avec first()
        agg_exprs = [first(c, ignorenulls=True).alias(c) for c in cols_to_agg]

        # GroupBy et agrégation
        parsed = df.groupBy("vin", "date").agg(*agg_exprs).coalesce(optimal_partitions_nb)

        return parsed
    
    def _get_dynamic_schema(self, field_def: dict, parse_type_map: dict):

        fields = []
        for key, value in field_def.items():
            fields.append(StructField(key, parse_type_map[value], True))
        return StructType(fields)


