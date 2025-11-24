import asyncio
import logging
import signal
import sys
from functools import partial

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
    SLACK_CHANNEL_ID,
    SLACK_TOKEN,
    STELLANTIS_BASE_URL,
    STELLANTIS_COMPANY_ID,
    STELLANTIS_EMAIL,
    STELLANTIS_PASSWORD,
    TESLA_BASE_URL,
    VW_AUTH_URL,
    VW_BASE_URL,
    VW_CLIENT_PASSWORD,
    VW_CLIENT_USERNAME,
    VW_ORGANIZATION_ID,
)
from activation.config.settings import LOGGING_CONFIG
from activation.fleet_info import check_vehicles_without_type
from activation.fleet_info import read_fleet_info as fleet_info
from activation.services.activation_service import VehicleActivationService
from activation.services.vehicle_processor import VehicleProcessor
from activation.utils.check_utils import ensure_admins_linked_to_fleets
from activation.utils.metric_utils import compare_active_vehicles, write_metrics_to_db
from core.slack_utils import send_slack_message

# Configure logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)


async def process_vehicles(owner_filter: str | None = None):
    """Main function to process vehicles in parallel."""
    try:
        # Initialize APIs
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
            base_url=TESLA_BASE_URL,
            slack_token=SLACK_TOKEN,
            slack_channel_id=SLACK_CHANNEL_ID,
        )

        # Get initial fleet info
        df = await fleet_info(fleet_filter=owner_filter)
        nbr_vehicles_without_type = check_vehicles_without_type(df)
        send_slack_message(
            SLACK_CHANNEL_ID, f"{nbr_vehicles_without_type} vehicles without type"
        )
        # Initialize activation service
        activation_service = VehicleActivationService(
            bmw_api=bmw_api,
            hm_api=hm_api,
            stellantis_api=stellantis_api,
            renault_api=renault_api,
            volkswagen_api=volkswagen_api,
            kia_api=kia_api,
            fleet_info_df=df,
        )

        # Process all brands in parallel
        await asyncio.gather(
            activation_service.activation_bmw(),
            activation_service.activation_hm(),
            activation_service.activation_stellantis(),
            activation_service.activation_volkswagen(),
            activation_service.activation_kia(),
        )

        # Get updated fleet info after activation"""
        logging.info(
            "-------------------------------Activation completed-------------------------------"
        )

        #   Process vehicles with updated info
        vehicle_processor = VehicleProcessor(
            bmw_api=bmw_api,
            hm_api=hm_api,
            stellantis_api=stellantis_api,
            renault_api=renault_api,
            volkswagen_api=volkswagen_api,
            kia_api=kia_api,
            tesla_api=tesla_api,
            df=df,
        )

        await asyncio.gather(
            vehicle_processor.process_tesla(),
            vehicle_processor.process_other_vehicles(),
            vehicle_processor.process_renault(),
            vehicle_processor.process_deactivated_vehicles(),
            vehicle_processor.process_bmw(),
        )

        await write_metrics_to_db(logger)
        await asyncio.to_thread(ensure_admins_linked_to_fleets, logger)
        db_not_in_gsheet, gsheet_not_in_db = await compare_active_vehicles(df)

        send_slack_message(
            SLACK_CHANNEL_ID,
            f"Les véhicules suivants sont présents dans la base de données mais pas dans le Gsheet: {db_not_in_gsheet}",
        )
        send_slack_message(
            SLACK_CHANNEL_ID,
            f"Les véhicules suivants sont présents dans le Gsheet mais pas dans la base de données: {gsheet_not_in_db}",
        )
    except Exception as e:
        logger.error(f"Error processing vehicles: {e!s}")
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
    try:
        # Create the main task
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(process_vehicles())

        # Set up signal handler with the current task
        signal.signal(signal.SIGINT, partial(handle_sigint, current_task=main_task))

        # Run the main task with cleanup
        loop.run_until_complete(cleanup(main_task))

    except KeyboardInterrupt:
        logger.info("\nProcess interrupted by user.")
    finally:
        # Close the event loop
        loop.close()
        sys.exit(0)
