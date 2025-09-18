from functools import reduce
from logging import Logger
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StructType
from ..config import PARSE_TYPE_MAP
from core.s3.s3_utils import S3Service

from transform.raw_tss.response_to_raw import ResponseToRawTss


class MobilisightResponseToRaw(ResponseToRawTss):
    """
    Class for processing data emitted by Mobilisight APIs
    stored in '/response/bmw/' on Scaleway
    """

    def __init__(
        self,
        make: str = "stellantis",
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

    def _build_fields_from_schema(
        self, schema: StructType, prefix: str = "", naming_sep: str = "."
    ) -> Dict[str, Dict]:
        """
        Generates a 'fields' dictionary for flattening extraction from a StructType schema,
        targeting ArrayType of StructType that contains a 'datetime' field.

        - Ignores fields named 'unit'
        - Keeps 'value' without suffix (e.g., 'odometer.value' → 'odometer')
        """
        result = {}

        for field in schema.fields:
            field_name = field.name
            full_path = f"{prefix}.{field_name}" if prefix else field_name

            if isinstance(field.dataType, ArrayType) and isinstance(
                field.dataType.elementType, StructType
            ):
                struct = field.dataType.elementType
                has_datetime = any(f.name == "datetime" for f in struct.fields)

                if has_datetime:
                    field_map = {}
                    for f in struct.fields:
                        if f.name in ("datetime", "unit"):
                            continue
                        if f.name == "value":
                            # Example: electricity.level.value → electricity_level
                            col_name = full_path.replace(".", naming_sep)
                        else:
                            # Example: electricity.level.percentage → electricity_level_percentage
                            col_name = f"{full_path.replace('.', naming_sep)}{naming_sep}{f.name}"
                        field_map[f.name] = col_name

                    result[full_path] = {"path": full_path, "fields": field_map}

                else:
                    # Recurse into nested structs
                    result.update(
                        self._build_fields_from_schema(struct, full_path, naming_sep)
                    )

            elif isinstance(field.dataType, StructType):
                # Simple (non-array) struct, go deeper
                result.update(
                    self._build_fields_from_schema(
                        field.dataType, full_path, naming_sep
                    )
                )

            else:
                # Non-struct, non-array field — ignore
                continue

        return result

    def parse_data(self, df: DataFrame, optimal_partitions_nb: int) -> DataFrame:
        """
        Parse dict from BMW API response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): active Spark session
            vin (str): Vehicle identification number

        Returns:
            spark.DataFrame: Data with all columns
        """

        df = df.coalesce(32)

        field_def = S3Service().read_yaml_file(f"config/{self.make}.yaml")["response_to_raw"]
        schema = self._get_dynamic_schema(field_def, PARSE_TYPE_MAP)

        fields = self._build_fields_from_schema(schema)

        # List to store all DataFrames in long format
        long_dfs = []
        for key, params in fields.items():
            path = params["path"]
            field_mapping = params["fields"]

            exploded = df.select("vin", F.explode_outer(path).alias("exploded_struct"))

            exploded = exploded.cache()


            # Create one row per field (excluding datetime and unit)
            for field_in_struct, alias in field_mapping.items():
                long_df = exploded.select(
                    "vin",
                    F.col("exploded_struct.datetime").alias("date"),
                    F.lit(alias).alias("key"),  # Column name
                    F.col(f"exploded_struct.{field_in_struct}")
                    .cast("string")
                    .alias("value"),
                ).dropna()

                long_dfs.append(long_df)
        exploded.unpersist()

        df_parsed = reduce(lambda a, b: a.unionByName(b), long_dfs)

        df_parsed = df_parsed.cache()
        # Force cache
        df_parsed.count()

        # Repartition to optimize pivot
        df_parsed = df_parsed.repartition("vin").coalesce(optimal_partitions_nb)

        # Pivot to get one column per key
        pivoted = (
            df_parsed.groupBy("vin", "date")
            .pivot("key")
            .agg(F.first("value"))
            .coalesce(optimal_partitions_nb)
        )

        df_parsed.unpersist()

        return pivoted
        


    def _get_dynamic_schema(self, field_def: dict, parse_type_map: dict):
        if isinstance(field_def, dict):
            # StructType
            fields = []
            for key, value in field_def.items():
                spark_type = self._get_dynamic_schema(value, parse_type_map)
                fields.append(StructField(key, spark_type, True))
            return StructType(fields)
        elif isinstance(field_def, list):
            # ArrayType
            if len(field_def) != 1 or not isinstance(field_def[0], dict):
                raise ValueError("Each array must contain exactly one dict describing the element structure.")
            return ArrayType(self._get_dynamic_schema(field_def[0], parse_type_map))
        elif isinstance(field_def, str):
            # Primitive type
            return parse_type_map[field_def.lower()]
        else:
            raise ValueError(f"Unsupported schema format, following is not a dict: {field_def}")
