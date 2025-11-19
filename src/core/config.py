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

# Database configuration has been moved to db_models.core.config
# All database URIs now include SSL encryption in transit for remote databases

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
