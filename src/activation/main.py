import asyncio
import logging
import signal
import sys
from functools import partial

import click

from activation.activators import (
    BMWActivator,
    HighMobilityActivator,
    KiaActivator,
    RenaultActivator,
    StellantisActivator,
    TeslaActivator,
    VolkswagenActivator,
)
from activation.api.bmw_client import BMWApi
from activation.api.hm_client import HMApi
from activation.api.kia_client import KiaApi
from activation.api.renault_client import RenaultApi
from activation.api.stellantis_client import StellantisApi
from activation.api.tesla_client import TeslaApi
from activation.api.volkswagen_client import VolkswagenApi
from activation.config.credentials import (
    BMW_AUTH_URL,
    BMW_BASE_URL,
    BMW_CLIENT_ID,
    BMW_CLIENT_PASSWORD,
    BMW_CLIENT_USERNAME,
    BMW_FLEET_ID,
    HM_BASE_URL,
    HM_CLIENT_ID,
    HM_CLIENT_SECRET,
    KIA_API_KEY,
    KIA_API_PWD,
    KIA_API_USERNAME,
    KIA_AUTH_URL,
    KIA_BASE_URL,
    RENAULT_AUD,
    RENAULT_CLIENT,
    RENAULT_KID,
    RENAULT_PWD,
    RENAULT_SCOPE,
    STELLANTIS_BASE_URL,
    STELLANTIS_COMPANY_ID,
    STELLANTIS_EMAIL,
    STELLANTIS_PASSWORD,
    TESLA_CLIENT_ID,
    TESLA_CLIENT_SECRET,
    VW_AUTH_URL,
    VW_BASE_URL,
    VW_CLIENT_PASSWORD,
    VW_CLIENT_USERNAME,
    VW_ORGANIZATION_ID,
)
from activation.config.settings import LOGGING_CONFIG
from activation.utils.check_utils import ensure_admins_linked_to_fleets
from activation.utils.metric_utils import write_metrics_to_db
from activation.vehicle_command import get_vehicle_command

# Configure logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)


async def activate_and_process_vehicles(oems=None):
    """Main function to process vehicles using OEM-specific activators.

    Args:
        oems: List of OEM names to process. If None, processes all OEMs.
              Valid values: tesla, bmw, renault, stellantis, volkswagen, kia, hm
    """

    try:
        bmw_api = BMWApi(
            auth_url=BMW_AUTH_URL,
            base_url=BMW_BASE_URL,
            client_id=BMW_CLIENT_ID,
            fleet_id=BMW_FLEET_ID,
            client_username=BMW_CLIENT_USERNAME,
            client_password=BMW_CLIENT_PASSWORD,
        )

        hm_api = HMApi(
            base_url=HM_BASE_URL, client_id=HM_CLIENT_ID, client_secret=HM_CLIENT_SECRET
        )

        stellantis_api = StellantisApi(
            base_url=STELLANTIS_BASE_URL,
            email=STELLANTIS_EMAIL,
            password=STELLANTIS_PASSWORD,
            company_id=STELLANTIS_COMPANY_ID,
        )

        kia_api = KiaApi(
            auth_url=KIA_AUTH_URL,
            base_url=KIA_BASE_URL,
            client_username=KIA_API_USERNAME,
            client_pwd=KIA_API_PWD,
            api_key=KIA_API_KEY,
        )

        renault_api = RenaultApi(
            kid=RENAULT_KID,
            aud=RENAULT_AUD,
            client=RENAULT_CLIENT,
            scope=RENAULT_SCOPE,
            pwd=RENAULT_PWD,
        )

        volkswagen_api = VolkswagenApi(
            auth_url=VW_AUTH_URL,
            base_url=VW_BASE_URL,
            organization_id=VW_ORGANIZATION_ID,
            client_username=VW_CLIENT_USERNAME,
            client_password=VW_CLIENT_PASSWORD,
        )
        tesla_api = TeslaApi(
            client_id=TESLA_CLIENT_ID,
            client_secret=TESLA_CLIENT_SECRET,
        )

        df = await get_vehicle_command()

        # Filter vehicles that need activation changes
        vehicle_command_df = df[
            df["activation_requested_status"] != df["activation_status"]
        ]

        # Get vehicles to process (activated but not yet processed)
        vehicle_to_process = df[
            (df.activation_status == True) & (df.is_processed == False)  # noqa: E712
        ]

        # Initialize all OEM activators
        logger.info("Initializing OEM activators...")

        all_activators = {
            "tesla": TeslaActivator(tesla_api, vehicle_command_df, vehicle_to_process),
            "bmw": BMWActivator(bmw_api, vehicle_command_df, vehicle_to_process),
            "renault": RenaultActivator(
                renault_api, vehicle_command_df, vehicle_to_process
            ),
            "stellantis": StellantisActivator(
                stellantis_api, vehicle_command_df, vehicle_to_process
            ),
            "volkswagen": VolkswagenActivator(
                volkswagen_api, vehicle_command_df, vehicle_to_process
            ),
            "kia": KiaActivator(kia_api, vehicle_command_df, vehicle_to_process),
            "hm": HighMobilityActivator(hm_api, vehicle_command_df, vehicle_to_process),
        }

        # Filter activators based on OEM selection
        if oems:
            oems_lower = [oem.lower() for oem in oems]
            activators_to_run = {
                name: activator
                for name, activator in all_activators.items()
                if name in oems_lower
            }
            if not activators_to_run:
                logger.error(f"No valid OEMs found in: {oems}")
                logger.info(f"Valid OEMs: {', '.join(all_activators.keys())}")
                return
            logger.info(
                f"Running activators for: {', '.join(activators_to_run.keys())}"
            )
        else:
            activators_to_run = all_activators
            logger.info("Running all OEM activators...")

        logger.info(
            f"Found {len(vehicle_to_process)} vehicles that need processing after activation"
        )

        # Run all activators in parallel (each handles activation + processing)
        logger.info("Starting parallel OEM activation and processing...")
        await asyncio.gather(
            *[activator.run() for activator in activators_to_run.values()]
        )

        logger.info(
            "-------------------------------All OEM activators completed-------------------------------"
        )

        if len(activators_to_run) == len(all_activators):
            logger.info("Writing activation metrics to database...")
            await write_metrics_to_db(logger)

        logger.info("Ensuring admins are linked to fleets...")
        await asyncio.to_thread(ensure_admins_linked_to_fleets, logger)

        logger.info("✅ Vehicle processing completed successfully!")

    except Exception as e:
        logger.error(f"Error processing vehicles: {e!s}")
        import traceback

        logger.error(traceback.format_exc())
        raise


def handle_sigint(signum, frame, current_task=None):
    """Handle SIGINT (Ctrl+C) gracefully."""
    if current_task:
        logger.info("\nReceived interrupt signal. Cancelling all tasks...")
        current_task.cancel()
    else:
        logger.info("\nReceived interrupt signal. Exiting...")
        sys.exit(0)


async def cleanup(task):
    """Clean up any remaining tasks."""
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Main task was cancelled. Cleaning up...")
    except Exception as e:
        logger.error(f"Error during cleanup: {e!s}")
    finally:
        # Cancel all remaining tasks
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        logger.info("All tasks completed. Exiting...")


if __name__ == "__main__":

    @click.command()
    @click.option(
        "--oem",
        "-o",
        multiple=True,
        type=click.Choice(
            ["tesla", "bmw", "renault", "stellantis", "volkswagen", "kia", "hm"],
            case_sensitive=False,
        ),
        help="OEM(s) to process. Can be specified multiple times. If not specified, all OEMs will be processed.",
    )
    def main(oem):
        """Vehicle Activation and Processing System.

        Process vehicle activations and enrichments for one or more OEMs.

        Examples:

            # Process all OEMs
            python main.py

            # Process only Tesla
            python main.py --oem tesla

            # Process Tesla and BMW
            python main.py --oem tesla --oem bmw

            # Short form
            python main.py -o tesla -o bmw
        """
        try:
            oems = list(oem) if oem else None

            loop = asyncio.get_event_loop()
            main_task = loop.create_task(activate_and_process_vehicles(oems))

            signal.signal(signal.SIGINT, partial(handle_sigint, current_task=main_task))

            loop.run_until_complete(cleanup(main_task))

        except KeyboardInterrupt:
            logger.info("\nProcess interrupted by user.")
        finally:
            loop.close()
            sys.exit(0)

    main()
