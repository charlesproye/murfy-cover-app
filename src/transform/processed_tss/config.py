from pandas.api.types import CategoricalDtype
from pyspark.sql.types import FloatType, TimestampType, StringType, BooleanType
from pandas import Timedelta as TD
import pandas as pd

S3_PROCESSED_TSS_KEY_FORMAT = 'processed_ts/{make}/time_series/processed_ts_spark.parquet'


ODOMETER_MILES_TO_KM = {
    "tesla": 1.60934,
    "tesla-fleet-telemetry": 1.60934,
}

# Coefficient to scale soc to 0-100%
SCALE_SOC = {
    'tesla-fleet-telemetry': 1,
    'mercedes-benz': 100,
    'volvo-cars': 100,
    'kia': 100,
    'renault': 100,
    'ford': 100,
    'stellantis': 100,
    'bmw': 1,
    'volkswagen': 1,
}


# Minimum threshold to consider a charging or discharging phase
SOC_DIFF_THRESHOLD = {
    'tesla-fleet-telemetry': 0.05,
    'mercedes-benz': 0.05,
    'volvo-cars': 0.05,
    'kia': 0.05,
    'renault': 0.05,
    'ford': 0.05,
    'stellantis': 0.05,
    'bmw': 0.05,
    'volkswagen': 0.05,
}




