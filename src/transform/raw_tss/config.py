from pandas import DataFrame as DF

S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_tss.parquet"
TESLA_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla")
DEFAULT_TESLA_RAW_TSS_DF = DF(columns=["vin", "readable_date"])

FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla-fleet-telemetry")

