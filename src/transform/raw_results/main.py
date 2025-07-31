import logging
import sys

from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.raw_results.processed_ts_to_raw_results import \
    ProcessedTsToRawResults
from transform.raw_results.providers.ford import FordProcessedTsToRawResults
from transform.raw_results.providers.renault import \
    RenaultProcessedTsToRawResults
from transform.raw_results.providers.stellantis import \
    StellantisProcessedTsToRawResults
from transform.raw_results.providers.tesla_fleet_telemetry import \
    TeslaFTProcessedTsToRawResults
from transform.raw_results.providers.volvo import VolvoProcessedTsToRawResults

ORCHESTRATED_MAKES = {
    "bmw": (False, ProcessedTsToRawResults),
    "mercedes-benz": (False, ProcessedTsToRawResults),
    "renault": (False, RenaultProcessedTsToRawResults),
    "volvo-cars": (False, VolvoProcessedTsToRawResults),
    "stellantis": (False, StellantisProcessedTsToRawResults),
    "kia": (False, ProcessedTsToRawResults),
    "ford": (False, FordProcessedTsToRawResults),
    "tesla-fleet-telemetry": (False, TeslaFTProcessedTsToRawResults),
    "volkswagen": (False, ProcessedTsToRawResults),
}


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("ProcessedTsToRawResults")
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    for make, (is_orchestrated, class_to_use) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            class_to_use(make=make, spark=spark, logger=logger).run()
        else:
            pass


if __name__ == "__main__":
    main()

