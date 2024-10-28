from os.path import join, dirname

# local paths
CSV_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.csv")
PARQUET_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.parquet")

# EV models

# s3 keys
S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_tss.parquet"
S3_PROCESSED_TSS_KEY_FORMAT = "processed_ts/{brand}/time_series/processed_tss.parquet"

SOH_LOST_PER_KM_DUMMY_RATIO = 0.000028

# caching
PATH_DOESNT_END_IN_PARQUET = "Extension of path '{path}' must be 'parquet'"
NO_BUCKET_ARG_FOUND = """
No value found for bucket to cache the result of the function {func_name}.
Using singleton bucket.
"""

# S3 data caching
KEY_LIST_COLUMN_NAMES = [ "key", "dtype_folder", 'brand', "vin", "file"]
EMTPY_S3_KEYS_WARNING_MSG = """
No responses found in {keys_prefix}.
"""
