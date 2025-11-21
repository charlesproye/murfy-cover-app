import logging

import click
from dagster_pipes import open_dagster_pipes

from core.models.make import MakeEnum
from transform.base_spark import BaseSpark
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

PROVIDERS: dict[MakeEnum, type[ProcessedPhaseToResultPhase]] = {
    MakeEnum.bmw: ProcessedPhaseToResultPhase,
    MakeEnum.mercedes_benz: ProcessedPhaseToResultPhase,
    MakeEnum.renault: RenaultProcessedPhaseToResultPhase,
    MakeEnum.volvo_cars: VolvoProcessedPhaseToResultPhase,
    MakeEnum.stellantis: StellantisProcessedPhaseToResultPhase,
    MakeEnum.kia: ProcessedPhaseToResultPhase,
    MakeEnum.ford: ProcessedPhaseToResultPhase,
    MakeEnum.tesla_fleet_telemetry: TeslaFTProcessedPhaseToResultPhase,
    MakeEnum.volkswagen: ProcessedPhaseToResultPhase,
}


class ProcessedPhaseToResultPhaseCLI(BaseSpark):
    def __init__(self):
        super().__init__(
            name="processed-phase-to-result-phase",
            help="Convert processed phases to result phases",
        )

    @click.argument("make", required=False)
    @click.option(
        "--all",
        "run_all",
        is_flag=True,
        help="Convert all makes",
    )
    def run(self, make: str, run_all: bool):
        """
        Run the conversion of raw TSS to processed phases.
        """
        if not run_all:
            if not make:
                raise click.UsageError("Please specify a make or use --all.")
            make = make.lower()
            try:
                make_enum = MakeEnum(make)
            except ValueError as err:
                raise click.BadParameter(f"Unknown make: {make}") from err
            if make_enum not in PROVIDERS:
                raise click.BadParameter(f"Unknown make: {make}")

        with open_dagster_pipes() as pipes:
            if run_all:
                for make_name, parser_class in PROVIDERS.items():
                    logger = logging.getLogger(
                        f"ProcessedPhaseToResultPhase[{make_name}]"
                    )
                    parser_class(
                        make=make_name,
                        spark=self.spark,
                        logger=logger,
                        force_update=True,
                        pipes=pipes,
                    ).run()
            else:
                logger = logging.getLogger(f"ProcessedPhaseToResultPhase[{make}]")
                PROVIDERS[make_enum](
                    make=make_enum,
                    spark=self.spark,
                    logger=logger,
                    force_update=True,
                    pipes=pipes,
                ).run()

    @staticmethod
    def list_makes():
        """List the available makes."""
        click.echo("Available makes:")
        for make in PROVIDERS:
            click.echo(f"  - {make}")

    def add_subcommands(self):
        self.cli.command("list-makes")(self.list_makes)


# Expose CLI for external access (e.g., Dagster Pipes)
cli = ProcessedPhaseToResultPhaseCLI().cli

if __name__ == "__main__":
    cli()
