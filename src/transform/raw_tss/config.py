from pandas import DataFrame as DF
from transform.raw_tss.parsing import parse_bmw, parse_mobilisight, parse_high_mobility, parse_fleet_telemetry

S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/spark_raw_tss.parquet"
TESLA_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla")
DEFAULT_TESLA_RAW_TSS_DF = DF(columns=["vin", "readable_date"])

FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(
    brand="tesla-fleet-telemetry"
)

SPARK_FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(
    brand="tesla-fleet-telemetry"
)

ALL_MAKES = [
    "tesla-fleet-telemetry",
    "bmw",
    "mercedes-benz",
    "ford",
    "volvo-cars",
    "stellantis",
    "kia",
    "renault",
]

GET_PARSING_FUNCTIONS = {
    # Stellantis
    "stellantis": parse_mobilisight,
    # BMW
    "bmw": parse_bmw,
    # Tesla
    # "tesla": tesla_get_raw_tss,
    # Kia
    "kia": parse_high_mobility,
    # Mercedes-Benz
    "mercedes-benz": parse_high_mobility,
    # Ford
    "ford": parse_high_mobility,
    # Renault
    "renault": parse_high_mobility,
    # Volvo
    "volvo-cars": parse_high_mobility,
    # fleet-telemetry
    "tesla-fleet-telemetry": parse_fleet_telemetry,
}

