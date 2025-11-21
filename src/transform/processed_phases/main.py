import logging

import click
from dagster_pipes import open_dagster_pipes

from core.models.make import MakeEnum
from transform.base_spark import BaseSpark
from transform.processed_phases.providers.bmw import BMWRawTsToProcessedPhases
from transform.processed_phases.providers.ford import FordRawTsToProcessedPhases
from transform.processed_phases.providers.kia import KiaRawTsToProcessedPhases
from transform.processed_phases.providers.mercedes_benz import (
    MercedesBenzRawTsToProcessedPhases,
)
from transform.processed_phases.providers.renault import RenaultRawTsToProcessedPhases
from transform.processed_phases.providers.stellantis import (
    StellantisRawTsToProcessedPhases,
)
from transform.processed_phases.providers.tesla_fleet_telemetry import (
    TeslaFTRawTsToProcessedPhases,
)
from transform.processed_phases.providers.volvo import VolvoRawTsToProcessedPhases
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases

PROVIDERS: dict[MakeEnum, type[RawTsToProcessedPhases]] = {
    MakeEnum.bmw: BMWRawTsToProcessedPhases,
    MakeEnum.mercedes_benz: MercedesBenzRawTsToProcessedPhases,
    MakeEnum.renault: RenaultRawTsToProcessedPhases,
    MakeEnum.volvo_cars: VolvoRawTsToProcessedPhases,
    MakeEnum.stellantis: StellantisRawTsToProcessedPhases,
    MakeEnum.kia: KiaRawTsToProcessedPhases,
    MakeEnum.ford: FordRawTsToProcessedPhases,
    MakeEnum.tesla_fleet_telemetry: TeslaFTRawTsToProcessedPhases,
    MakeEnum.volkswagen: RawTsToProcessedPhases,
}


class RawTsToProcessedPhasesCLI(BaseSpark):
    def __init__(self):
        super().__init__(
            name="raw-ts-to-processed-phases",
            help="Convert raw TSS to processed phases",
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
                    logger = logging.getLogger(f"RawTsToProcessedPhases[{make_name}]")
                    parser_class(
                        make=make_name,
                        spark=self.spark,
                        logger=logger,
                        force_update=True,
                        pipes=pipes,
                    ).run()
            else:
                logger = logging.getLogger(f"RawTsToProcessedPhases[{make}]")
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
cli = RawTsToProcessedPhasesCLI().cli

if __name__ == "__main__":
    cli()
