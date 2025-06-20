from pandas import DataFrame as DF
from transform.raw_tss.parsing import * 


S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/spark_raw_tss.parquet"
TESLA_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla")
DEFAULT_TESLA_RAW_TSS_DF = DF(columns=["vin", "readable_date"])

FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla-fleet-telemetry")

SPARK_FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla-fleet-telemetry")

ALL_MAKES = ["tesla-fleet-telemetry",
             "bmw",
             "mercedes-benz",
             "ford",
             "volvo-cars",
             "stellantis",
             "kia",
             "renault",
]

LIST_COL_TO_DROP = ["model"]

GET_PARSING_FUNCS = {
    #"bmw": ,
    #"stellantis": ,
    "mercedes-benz": parse_high_mobility,
    "kia": parse_high_mobility,
    "renault": parse_high_mobility,
    "volvo": parse_high_mobility,
    "ford": parse_high_mobility,
    "tesla-fleet-telemetry": parse_fleet_telemetry 
}
