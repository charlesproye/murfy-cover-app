import logging
import sys

from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.result_phases.processed_phase_to_result_phase import (
    ProcessedPhaseToResultPhase,
)
from transform.result_phases.providers.renault import RenaultProcessedPhaseToResultPhase
from transform.result_phases.providers.stellantis import (
    StellantisProcessedPhaseToResultPhase,
)
from transform.result_phases.providers.tesla import TeslaProcessedPhaseToResultPhase
from transform.result_phases.providers.tesla_fleet_telemetry import (
    TeslaFTProcessedPhaseToResultPhase,
)
from transform.result_phases.providers.volvo import VolvoProcessedPhaseToResultPhase

ORCHESTRATED_MAKES = {
    "bmw": (False, ProcessedPhaseToResultPhase),
    "mercedes-benz": (False, ProcessedPhaseToResultPhase),
    "renault": (False, RenaultProcessedPhaseToResultPhase),
    "volvo-cars": (False, VolvoProcessedPhaseToResultPhase),
    "stellantis": (False, StellantisProcessedPhaseToResultPhase),
    "kia": (False, ProcessedPhaseToResultPhase),
    "ford": (False, ProcessedPhaseToResultPhase),
    "tesla-fleet-telemetry": (True, TeslaFTProcessedPhaseToResultPhase),
    "volkswagen": (True, ProcessedPhaseToResultPhase),
    "tesla": (False, TeslaProcessedPhaseToResultPhase),
}


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("ProcessedPhaseToResultPhase")
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    for make, (is_orchestrated, class_to_use) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            class_to_use(make=make, spark=spark, logger=logger, force_update=True)
        else:
            pass


if __name__ == "__main__":
    main()

