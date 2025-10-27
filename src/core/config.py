"""
Stores all the variables used by the other core modules that would hurt the readablity of the code.
This includes Warning/Error messages, path to files, ect...
"""

from os.path import dirname, join

import pandas as pd

# local paths
CSV_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.csv")
PARQUET_EV_MODELS_INFO_PATH = join(dirname(__file__), "data_cache/models_info.parquet")

# S3 data caching
KEY_LIST_COLUMN_NAMES = ["key", "dtype_folder", "brand", "vin", "file"]
EMTPY_S3_KEYS_WARNING_MSG = "No responses found in {keys_prefix}."

# Data caching
NO_BUCKET_ARG_FOUND = "No bucket argument found in function {func_name}"
PATH_DOESNT_END_IN_PARQUET = "Path '{path}' doesn't end in '.parquet'"

# model info
MODEL_INF_SQL_QUERY = """
SELECT *
FROM vehicle_model
JOIN oem
ON vehicle_model.oem_id = oem.id;
"""
MODEL_INFO_DTYPES = {
    "manufacturer": "string",
    "model": "string",
    "version": "string",
    "tesla_code": "string",
    "capacity": "float",
    "net_capacity": "float",
    "range": "float",
}
MODEL_INFO_NAME_MAP = {
    "oem_name": "manufacturer",
    "model_name": "model",
    "version": "tesla_code",
    "type": "version",
    "capacity": "capacity",
    "net_capacity": "net_capacity",
    "autonomy": "range",
}

# SQL
DB_URI_FORMAT_KEYS = [
    "DB_DATA_EV_USER",
    "DB_DATA_EV_HOST",
    "DB_DATA_EV_PASSWORD",
    "DB_DATA_EV_PORT",
    "DB_DATA_EV_NAME",
]

DB_ENG_URI_FORMAT_KEYS = [
    "DB_DATA_ENG_USER",
    "DB_DATA_ENG_HOST",
    "DB_DATA_ENG_PASSWORD",
    "DB_DATA_ENG_PORT",
    "DB_DATA_ENG_NAME",
]

DB_URI_FORMAT_KEYS_PROD = [
    "PROD_DATA_EV_USER",
    "PROD_DATA_EV_HOST",
    "PROD_DATA_EV_PASSWORD",
    "PROD_DATA_EV_PORT",
    "PROD_DATA_EV_NAME",
]
DB_URI_FORMAT_STR = "postgresql+psycopg2://{DB_DATA_EV_USER}:{DB_DATA_EV_PASSWORD}@{DB_DATA_EV_HOST}:{DB_DATA_EV_PORT}/{DB_DATA_EV_NAME}"
DB_URI_FORMAT_STR_PROD = "postgresql+psycopg2://{PROD_DATA_EV_USER}:{PROD_DATA_EV_PASSWORD}@{PROD_DATA_EV_HOST}:{PROD_DATA_EV_PORT}/{PROD_DATA_EV_NAME}"
DB_ENG_URI_FORMAT_STR = "postgresql+psycopg2://{DB_DATA_ENG_USER}:{DB_DATA_ENG_PASSWORD}@{DB_DATA_ENG_HOST}:{DB_DATA_ENG_PORT}/{DB_DATA_ENG_NAME}"

valid_soh_points = pd.DataFrame(
    {
        "odometer": [20_000, 200_000, 0, 200_000],
        "soh": [1.0, 0.95, 0.9, 0.6],
        "point": ["A", "B", "A", "B"],
        "bound": ["max", "max", "min", "min"],
    }
).set_index(["bound", "point"])


# SoH estimation evaluation
BASE_SLOPE = 0.08 / 1e4  # base soh loss per kilometer (0.8% per 10k km)

