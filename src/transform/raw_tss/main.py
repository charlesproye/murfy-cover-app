import logging
import os
import sys

import click

from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session, create_spark_session_k8s
from transform.raw_tss.providers.bmw import BMWResponseToRaw
from transform.raw_tss.providers.high_mobility import HighMobilityResponseToRaw
from transform.raw_tss.providers.kia import KiaResponseToRaw
from transform.raw_tss.providers.mobilisight import MobilisightResponseToRaw
from transform.raw_tss.providers.tesla import TeslaResponseToRaw
from transform.raw_tss.providers.tesla_fleet_telemetry import TeslaFTResponseToRawTss
from transform.raw_tss.providers.volkswagen import VolkswagenResponseToRaw

PROVIDERS = {
    "bmw": BMWResponseToRaw,
    "mercedes-benz": HighMobilityResponseToRaw,
    "renault": HighMobilityResponseToRaw,
    "volvo-cars": HighMobilityResponseToRaw,
    "stellantis": MobilisightResponseToRaw,
    "kia": HighMobilityResponseToRaw,
    "ford": HighMobilityResponseToRaw,
    "tesla-fleet-telemetry": TeslaFTResponseToRawTss,
    "volkswagen": VolkswagenResponseToRaw,
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
    help="Execute the parsing for all makes",
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
            logger = logging.getLogger(f"ResponseToRawTss[{make_name}]")
            parser_class(make=make_name, spark=spark, logger=logger).run()

    else:
        if not make:
            raise click.UsageError("Veuillez préciser une marque ou utiliser --all.")
        make = make.lower()
        if make not in PROVIDERS:
            raise click.BadParameter(f"Marque inconnue: {make}")

        logger = logging.getLogger(f"ResponseToRawTss[{make}]")
        parser_class = PROVIDERS[make]
        parser_class(make=make, spark=spark, logger=logger).run()


@cli.command("list-makes")
def list_makes():
    """Affiche les marques disponibles."""
    click.echo("Marques disponibles :")
    for make in PROVIDERS:
        click.echo(f"  - {make}")


if __name__ == "__main__":
    cli()

