from datetime import timedelta as TD
from os.path import join, dirname

KJ_TO_KWH = 0.00027777778
MILES_TO_KM = 1.60934
KJ_TO_KWH = 0.00027777777

# paths
CSV_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.csv")
PARQUET_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.parquet")


S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_tss.parquet"
S3_PROCESSED_TSS_KEY_FORMAT = "processed_ts/{brand}/time_series/processed_tss.parquet"


SOH_LOST_PER_KM_DUMMY_RATIO = 0.000028

# caching
PATH_DOESNT_END_IN_PARQUET = "Extension of path '{path}' must be 'parquet'"

# S3 data caching
KEY_LIST_COLUMN_NAMES = [ "key", "dtype_folder", 'brand', "vin", "file"]
EMTPY_S3_KEYS_WARNING_MSG = """
No responses found in {keys_prefix}.
"""
