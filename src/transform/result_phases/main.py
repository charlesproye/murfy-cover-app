import logging
import os
import sys

import click

from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session, create_spark_session_k8s
from transform.result_phases.processed_phase_to_result_phase import (
    ProcessedPhaseToResultPhase,
)
from transform.result_phases.providers.renault import RenaultProcessedPhaseToResultPhase
from transform.result_phases.providers.stellantis import (
    StellantisProcessedPhaseToResultPhase,
)
from transform.result_phases.providers.tesla_fleet_telemetry import (
    TeslaFTProcessedPhaseToResultPhase,
)
from transform.result_phases.providers.volvo import VolvoProcessedPhaseToResultPhase

PROVIDERS = {
    "bmw": ProcessedPhaseToResultPhase,
    "mercedes-benz": ProcessedPhaseToResultPhase,
    "renault": RenaultProcessedPhaseToResultPhase,
    "volvo-cars": VolvoProcessedPhaseToResultPhase,
    "stellantis": StellantisProcessedPhaseToResultPhase,
    "kia": ProcessedPhaseToResultPhase,
    "ford": ProcessedPhaseToResultPhase,
    "tesla-fleet-telemetry": TeslaFTProcessedPhaseToResultPhase,
    "volkswagen": ProcessedPhaseToResultPhase,
}


@click.group()
def cli():
    pass


@cli.command()
@click.argument("make", required=False)
@click.option(
    "--all",
    "run_all",
    is_flag=True,
    help="Execute the processing for all makes",
)
def run(make: str, run_all: bool):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    settings = S3Settings()
    is_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None

    if is_k8s:
        spark = create_spark_session_k8s(settings.S3_KEY, settings.S3_SECRET)
    else:
        spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    if run_all:
        for make_name, parser_class in PROVIDERS.items():
            logger_make = logging.getLogger(f"ProcessedPhaseToResultPhase[{make_name}]")
            parser_class(
                make=make_name,
                spark=spark,
                logger=logger_make,
                force_update=True,
            ).run()
    else:
        if not make:
            raise click.UsageError("Veuillez préciser une marque")
        make = make.lower()
        if make not in PROVIDERS:
            raise click.BadParameter(f"Marque inconnue: {make}")
        logger_make = logging.getLogger(f"ProcessedPhaseToResultPhase[{make}]")
        parser_class = PROVIDERS[make]
        parser_class(
            make=make,
            spark=spark,
            logger=logger_make,
            force_update=True,
        ).run()


if __name__ == "__main__":
    cli()

