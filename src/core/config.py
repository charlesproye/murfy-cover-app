from os.path import join, dirname

# local paths
CSV_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.csv")
PARQUET_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.parquet")

# S3 data caching
KEY_LIST_COLUMN_NAMES = [ "key", "dtype_folder", 'brand', "vin", "file"]
EMTPY_S3_KEYS_WARNING_MSG = """
No responses found in {keys_prefix}.
"""

# Data caching
NO_BUCKET_ARG_FOUND = "No bucket argument found in function {func_name}"
PATH_DOESNT_END_IN_PARQUET = "Path '{path}' doesn't end in '.parquet'"

# SQL
DB_URI_FORMAT_KEYS = [
    "DB_USERNAME",
    "DB_ADRESSE",
    "DB_PASSWORD",
    "DB_PORT",
    "DB_DATA_NAME",
]
DB_URI_FORMAT_STR = "postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_ADRESSE}:{DB_PORT}/{DB_DATA_NAME}"

