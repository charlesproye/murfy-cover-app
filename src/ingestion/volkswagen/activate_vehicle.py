"""To run the script cd at the root of the repository and run
python -m src.ingestion.volkswagen.activate_vehicle path_to_your_vin_file
"""

import logging
from .client import VolksWagenClient
from .schemas import VehicleConfirmation
import asyncio
import click
from typing import TextIO


@click.command(
    help="Reads a file with a list of strings and processes each line. give the path to a txt file with the list of vins, one on each line to add them. Don't try to add to much vins at the same time (the documentation recommend a maximum of 1000 vins)"
)
@click.argument("input_file", type=click.File("r"))
def main(input_file: TextIO):
    asyncio.run(async_main(input_file))


async def async_main(input_file: TextIO):
    vins = [line.strip() for line in input_file]
    VW_CLIENT = VolksWagenClient()
    r = await VW_CLIENT.post_vehicle_approval(vins)
    confirmation_codes: list[dict] = r.json()
    confirmations = [
        VehicleConfirmation(
            vin=code["vin"], verification_code=code["vehicle-verification-codes"]
        )
        for code in confirmation_codes
    ]
    r = await VW_CLIENT.post_vehicle_confirmation(confirmations)
    logging.info(f"Response of the confirmation {r.status_code = }, {r.text = }")


if __name__ == "__main__":
    main()

