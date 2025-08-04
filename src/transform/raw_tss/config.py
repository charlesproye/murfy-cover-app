from pyspark.sql.types import *

S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_ts_spark.parquet"

PARSE_TYPE_MAP = {
    "str": StringType(),
    "int": IntegerType(),
    "float": DoubleType(),
    "datetime": TimestampType(),
    "timestamp": TimestampType(),
    "boolean": BooleanType(),
    "double": DoubleType(),
    "string": StringType(),
    "long": LongType(),
    "integer": IntegerType(),
    "timestamp": TimestampType()
}

