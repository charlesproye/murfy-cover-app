import logging
import sys

from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.raw_tss.providers.bmw import BMWResponseToRaw
from transform.raw_tss.providers.high_mobility import HighMobilityResponseToRaw
from transform.raw_tss.providers.mobilisight import MobilisightResponseToRaw
from transform.raw_tss.providers.tesla_fleet_telemetry import TeslaFTResponseToRawTss
from transform.raw_tss.providers.volkswagen import VolkswagenResponseToRaw
from transform.raw_tss.providers.tesla import TeslaResponseToRaw

ORCHESTRATED_MAKES = {
    "bmw": (True, BMWResponseToRaw),
    "mercedes-benz": (True, HighMobilityResponseToRaw),
    "renault": (True, HighMobilityResponseToRaw),
    "volvo-cars": (True, HighMobilityResponseToRaw),
    "stellantis": (True, MobilisightResponseToRaw),
    "kia": (True, HighMobilityResponseToRaw),
    "ford": (True, HighMobilityResponseToRaw),
    "tesla-fleet-telemetry": (True, TeslaFTResponseToRawTss),
    "volkswagen": (True, VolkswagenResponseToRaw),
    "tesla": (False, TeslaResponseToRaw),
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

