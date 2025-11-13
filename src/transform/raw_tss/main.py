import logging

import click
from dagster_pipes import open_dagster_pipes

from core.models.make import MakeEnum
from transform.base_spark import BaseSpark
from transform.raw_tss.providers.bmw import BMWResponseToRaw
from transform.raw_tss.providers.high_mobility import HighMobilityResponseToRaw
from transform.raw_tss.providers.kia import KiaResponseToRaw
from transform.raw_tss.providers.mobilisight import MobilisightResponseToRaw
from transform.raw_tss.providers.tesla_fleet_telemetry import TeslaFTResponseToRawTss
from transform.raw_tss.providers.volkswagen import VolkswagenResponseToRaw
from transform.raw_tss.response_to_raw import ResponseToRawTss

PROVIDERS: dict[MakeEnum, type[ResponseToRawTss]] = {
    MakeEnum.bmw: BMWResponseToRaw,
    MakeEnum.mercedes_benz: HighMobilityResponseToRaw,
    MakeEnum.renault: HighMobilityResponseToRaw,
    MakeEnum.volvo_cars: HighMobilityResponseToRaw,
    MakeEnum.stellantis: MobilisightResponseToRaw,
    MakeEnum.kia: KiaResponseToRaw,
    MakeEnum.ford: HighMobilityResponseToRaw,
    MakeEnum.tesla_fleet_telemetry: TeslaFTResponseToRawTss,
    MakeEnum.volkswagen: VolkswagenResponseToRaw,
}


class ResponseToRawTssCLI(BaseSpark):
    def __init__(self):
        super().__init__(
            name="response-to-raw-tss", help="Convert raw data from APIs to raw TSS"
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
        Run the conversion of raw data from APIs to raw TSS.
        """
        if not run_all:
            if not make:
                raise click.UsageError("Please specify a make or use --all.")
            make = make.lower()
            if make not in PROVIDERS:
                raise click.BadParameter(f"Unknown make: {make}")

        with open_dagster_pipes() as pipes:
            if run_all:
                for make_name, parser_class in PROVIDERS.items():
                    logger = logging.getLogger(f"ResponseToRawTss[{make_name}]")
                    parser_class(make=make_name, spark=self.spark, logger=logger).run(
                        pipes
                    )
            else:
                logger = logging.getLogger(f"ResponseToRawTss[{make}]")
                PROVIDERS[make](make=make, spark=self.spark, logger=logger).run(pipes)

    @staticmethod
    def list_makes():
        """List the available makes."""
        click.echo("Available makes:")
        for make in PROVIDERS:
            click.echo(f"  - {make}")

    def add_subcommands(self):
        self.cli.command("list-makes")(self.list_makes)


# Expose CLI for external access (e.g., Dagster Pipes)
cli = ResponseToRawTssCLI().cli

if __name__ == "__main__":
    cli()
