from os.path import join, dirname

# local paths
CSV_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.csv")
PARQUET_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.parquet")

SOH_LOST_PER_KM_DUMMY_RATIO = 0.000028
# S3 data caching
KEY_LIST_COLUMN_NAMES = [ "key", "dtype_folder", 'brand', "vin", "file"]
EMTPY_S3_KEYS_WARNING_MSG = """
No responses found in {keys_prefix}.
"""

# SQL
DB_URI_FORMAT_KEYS = [
    "DB_USERNAME",
    "DB_ADRESSE",
    "DB_PASSWORD",
    "DB_PORT",
    "DB_DATA_NAME",
]
DB_URI_FORMAT_STR = "postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_ADRESSE}:{DB_PORT}/{DB_DATA_NAME}"

