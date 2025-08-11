import logging
import sys
from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.processed_phases.providers.renault import RenaultRawTsToProcessedPhases

ORCHESTRATED_MAKES = {
    "bmw": (False, None),
    "mercedes-benz": (False, None),
    "renault": (True, RenaultRawTsToProcessedPhases),
    "volvo-cars": (False, None),
    "stellantis": (False, None),
    "kia": (False, None),
    "ford": (False, None),
    "tesla-fleet-telemetry": (False, None),
    "volkswagen": (False, None),
}


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("RawTsToProcessedPhases")
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    for make, (is_orchestrated, class_to_use) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            class_to_use(make=make, spark=spark, logger=logger, force_update=True)
        else:
            pass


if __name__ == "__main__":
    main()
