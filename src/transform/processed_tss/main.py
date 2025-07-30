import logging
import sys

from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.processed_tss.providers.bmw import BMWRawTsToProcessedTs
from transform.processed_tss.providers.high_mobility import \
    HighMobilityRawTsToProcessedTs
from transform.processed_tss.providers.mobilisight import \
    MobilisightRawTsToProcessedTs
from transform.processed_tss.providers.tesla_fleet_telemetry import \
    TeslaFTRawTsToProcessedTs
from transform.processed_tss.providers.volkswagen import \
    VolkswagenRawTsToProcessedTs

ORCHESTRATED_MAKES = {
    "bmw": (False, BMWRawTsToProcessedTs),
    "mercedes-benz": (False, HighMobilityRawTsToProcessedTs),
    "renault": (False, HighMobilityRawTsToProcessedTs),
    "volvo-cars": (False, HighMobilityRawTsToProcessedTs),
    "stellantis": (False, MobilisightRawTsToProcessedTs),
    "kia": (False, HighMobilityRawTsToProcessedTs),
    "ford": (False, HighMobilityRawTsToProcessedTs),
    "tesla-fleet-telemetry": (True, TeslaFTRawTsToProcessedTs),
    "volkswagen": (False, VolkswagenRawTsToProcessedTs),
}


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("RawTsToProcessedTs")
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    for make, (is_orchestrated, class_to_use) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            class_to_use(make=make, spark=spark, logger=logger, force_update=True)
        else:
            pass


if __name__ == "__main__":
    main()

