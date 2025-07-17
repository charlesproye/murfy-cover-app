from datetime import datetime, timedelta
from functools import reduce
from logging import Logger
from typing import Optional

from transform.raw_tss.config import SCHEMAS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, explode, expr, input_file_name, lit,
                                   regexp_extract, size, udf)
from pyspark.sql.types import ArrayType, StringType, StructType, TimestampType

from transform.raw_tss.base.response_to_raw import ResponseToRawTss
from transform.raw_tss.providers.utils import get_next_scheduled_timestamp


class HighMobilityResponseToRaw(ResponseToRawTss):
    """
    Classe pour traiter les données renvoyées par les API Tesla Fleet Telemetry
    stockées dans /response sur Scaleway
    """

    def __init__(
        self,
        make: str = "",
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
        Parse dict from High Mobility api response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): spark session active
            vin (str): Vehicle identification number

        Returns:
            spark.DataFrame: Data with every columns
        """

        get_next_scheduled_timestamp_udf = udf(
            get_next_scheduled_timestamp, TimestampType()
        )

        df = df.coalesce(optimal_partitions_nb)
        df = df.withColumn("filepath", input_file_name())
        df = df.withColumn(
            "vin", regexp_extract("filepath", r"/([^/]+)/\d{4}-\d{2}-\d{2}\.json$", 1)
        )
        df = df.withColumn("vin", col("vin").cast(StringType())).drop("filepath")
        df = df.repartition("vin").coalesce(optimal_partitions_nb)

        def infer_data_path(array_field: ArrayType):
            """
            Détermine si le champ `data` est un struct contenant un champ `value`, ou un scalaire.
            """
            if isinstance(array_field.elementType, StructType):
                for f in array_field.elementType.fields:
                    if f.name == "data":
                        data_field = f.dataType
                        if isinstance(data_field, StructType):
                            # On regarde s’il contient un champ "value"
                            if "value" in [sub.name for sub in data_field.fields]:
                                return "data.value"
                            else:
                                return "custom"  # ex: time.hour/minute
                        else:
                            return "data"
            return "unknown"

        signals = []

        full_schema = SCHEMAS[self.make]

        for top_field in full_schema.fields:
            domain = top_field.name
            if isinstance(top_field.dataType, StructType):
                for signal_field in top_field.dataType.fields:
                    signal_name = signal_field.name
                    signal_type = signal_field.dataType

                    if isinstance(signal_type, ArrayType):
                        data_path = infer_data_path(signal_type)
                        signals.append((domain, signal_name, data_path))

        df.cache()

        dfs = []

        for parent_col, signal_name, value_path in signals:
            nested_col = col(f"{parent_col}.{signal_name}")

            col_path = f"{parent_col}.{signal_name}"
            # On vérifie que la colonne existe et contient au moins une entrée non vide
            if (
                df.select(size(col(col_path)).alias("size"))
                .filter("size > 0")
                .limit(1)
                .count()
                > 0
            ):
                if value_path == "custom":
                    # ✅ Logique pour les timestamps avec get_next_scheduled_timestamp
                    exploded = (
                        df.filter(col(parent_col).isNotNull())
                        .select("vin", explode(nested_col).alias("entry"))
                        .select(
                            col("vin"),
                            col("entry.timestamp").alias("date"),
                            lit(signal_name).alias("signal"),
                            # Utiliser get_next_scheduled_timestamp pour les données custom
                            get_next_scheduled_timestamp_udf(
                                col("entry.timestamp"), col("entry.data")
                            ).alias("value"),
                        )
                    )
                else:
                    # Logique normale pour data.value
                    exploded = (
                        df.filter(col(parent_col).isNotNull())
                        .select("vin", explode(nested_col).alias("entry"))
                        .select(
                            col("vin"),
                            col("entry.timestamp").alias("date"),
                            lit(signal_name).alias("signal"),
                            col(f"entry.{value_path}").alias("value"),
                        )
                    )
                dfs.append(exploded)
        # Union de tous les signaux
        parsed_df = reduce(lambda a, b: a.unionByName(b), dfs)

        pivoted = (
            parsed_df.repartition("vin")
            .groupBy("vin", "date")
            .pivot("signal")
            .agg(expr("first(value)"))
            .coalesce(optimal_partitions_nb)
        )

        return pivoted

