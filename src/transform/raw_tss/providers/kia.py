from datetime import datetime
from logging import Logger

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from transform.raw_tss.config import PARSE_TYPE_MAP
from transform.raw_tss.response_to_raw import ResponseToRawTss


class KiaResponseToRaw(ResponseToRawTss):
    """
    Class for processing data emitted by Kia APIs
    stored in '/response/kia/' on Scaleway
    """

    def __init__(
        self,
        make: str = "kia",
        force_update: bool = False,
        writing_mode: str | None = "append",
        spark: SparkSession | None = None,
        logger: Logger | None = None,
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
        Parse dict from KIA API response and pivot to vin, date, values.

        Args:
            df (DataFrame): Spark DataFrame containing columns meta, state, header
            optimal_partitions_nb (int): Number of partitions to repartition the RDD for performance

        Returns:
            DataFrame: Spark DataFrame with columns vin, date, and all value columns
        """

        def row_to_long_format(row):
            if row.state is None or row.meta is None or row.header is None:
                return []

            vin = row.header.vin
            state = row.state.asDict(recursive=True)
            meta = row.meta.asDict(recursive=True)

            results = []

            def match_state_and_meta(state_d, meta_d, prefix=""):
                for k, v in state_d.items():
                    name = f"{prefix}{k}" if prefix == "" else f"{prefix}_{k}"
                    meta_val = meta_d.get(k, None) if meta_d else None
                    if isinstance(v, dict):
                        meta_sub = meta_val if isinstance(meta_val, dict) else {}
                        match_state_and_meta(v, meta_sub, name)
                    else:
                        if isinstance(meta_val, (int, float)):
                            ts = datetime.utcfromtimestamp(meta_val / 1000)
                            try:
                                value = float(v)
                            except (ValueError, TypeError):
                                value = None
                            results.append(
                                Row(vin=vin, date=ts, variable=name, value=value)
                            )

            match_state_and_meta(state, meta)
            return results

        rdd_long = df.rdd.flatMap(row_to_long_format).repartition(optimal_partitions_nb)

        df_long = rdd_long.toDF()

        pivoted = df_long.groupBy("vin", "date").pivot("variable").agg(F.first("value"))

        return pivoted

    def _get_dynamic_schema(self, field_def: dict, parse_type_map: dict) -> StructType:
        """
        Transforme un dictionnaire de type {key: type_or_nested_dict} en StructType Spark.
        """

        def parse_field(name, value):
            if isinstance(value, dict):
                return StructField(
                    name, self._get_dynamic_schema(value, parse_type_map), True
                )
            else:
                if value not in PARSE_TYPE_MAP:
                    raise ValueError(f"Type inconnu pour le champ {name}: {value}")
                return StructField(name, parse_type_map[value], True)

        fields = [parse_field(k, v) for k, v in field_def.items()]
        return StructType(fields)

