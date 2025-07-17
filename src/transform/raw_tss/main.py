import logging
import sys

from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.raw_tss.BMWResponseToRaw import BMWResponseToRaw
from transform.raw_tss.HighMobilityResponseToRaw import \
    HighMobilityResponseToRaw
from transform.raw_tss.MobilisightResponseToRaw import MobilisightResponseToRaw
from transform.raw_tss.TeslaFTResponseToRawTss import TeslaFTResponseToRawTss

ORCHESTRATED_MAKES = {
    "bmw": (False, BMWResponseToRaw),
    "mercedes-benz": (True, HighMobilityResponseToRaw),
    "renault": (False, HighMobilityResponseToRaw),
    "volvo-cars": (False, HighMobilityResponseToRaw),
    "stellantis": (False, MobilisightResponseToRaw),
    "kia": (False, HighMobilityResponseToRaw),
    "ford": (False, HighMobilityResponseToRaw),
    "tesla-fleet-telemetry": (False, TeslaFTResponseToRawTss),
}


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("ResponseToRawTss")
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    for make, (is_orchestrated, class_to_use) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            class_to_use(make=make, spark=spark, logger=logger).run()
        else:
            pass


if __name__ == "__main__":
    main()

