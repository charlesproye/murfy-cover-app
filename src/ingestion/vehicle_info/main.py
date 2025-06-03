import asyncio
import logging
import signal
import sys
from typing import Optional
from functools import partial

from ingestion.vehicle_info.config.settings import LOGGING_CONFIG
from ingestion.vehicle_info.config.credentials import *
from ingestion.vehicle_info.api.bmw_client import BMWApi
from ingestion.vehicle_info.api.hm_client import HMApi
from ingestion.vehicle_info.api.stellantis_client import StellantisApi
from ingestion.vehicle_info.api.tesla_client import TeslaApi
from ingestion.vehicle_info.api.tesla_particulier import TeslaParticulierApi
from ingestion.vehicle_info.services.activation_service import VehicleActivationService
from ingestion.vehicle_info.services.vehicle_processor import VehicleProcessor
from ingestion.vehicle_info.fleet_info import read_fleet_info as fleet_info
from ingestion.vehicle_info.api.renault_client import RenaultApi
# Configure logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)

async def process_vehicles(owner_filter: Optional[str] = None):
    """Main function to process vehicles in parallel."""
    try:
        # Initialize APIs
        bmw_api = BMWApi(
            auth_url=BMW_AUTH_URL,
            base_url=BMW_BASE_URL,
            client_id=BMW_CLIENT_ID,
            fleet_id=BMW_FLEET_ID,
            client_username=BMW_CLIENT_USERNAME,
            client_password=BMW_CLIENT_PASSWORD
        )
        hm_api = HMApi(
            base_url=HM_BASE_URL,
            client_id=HM_CLIENT_ID,
            client_secret=HM_CLIENT_SECRET
        )
        stellantis_api = StellantisApi(
            base_url=STELLANTIS_BASE_URL,
            email=STELLANTIS_EMAIL,
            password=STELLANTIS_PASSWORD,
            fleet_id=STELLANTIS_FLEET_ID,
            company_id=STELLANTIS_COMPANY_ID
        )
        tesla_api = TeslaApi(
            base_url=TESLA_BASE_URL,
            slack_token=SLACK_TOKEN,
            slack_channel_id=SLACK_CHANNEL_ID
        )
        tesla_particulier_api = TeslaParticulierApi(
            base_url=TESLA_BASE_URL,
            token_url= TESLA_TOKEN_URL,
            client_id=TESLA_CLIENT_ID,
        )
        renault_api = RenaultApi(
            kid=RENAULT_KID,
            aud=RENAULT_AUD,
            client=RENAULT_CLIENT,
            scope=RENAULT_SCOPE,
            pwd=RENAULT_PWD
        )
        # Get initial fleet info
        df = await fleet_info(owner_filter=owner_filter)
        # Initialize activation service
        activation_service = VehicleActivationService(
            bmw_api=bmw_api,
            hm_api=hm_api,
            stellantis_api=stellantis_api,
            tesla_api=tesla_api,
            tesla_particulier_api=tesla_particulier_api,
            renault_api=renault_api,
            fleet_info_df=df
        )

        # Process all brands in parallel
        await asyncio.gather(
            #activation_service.activation_tesla(),
            activation_service.activation_bmw(),
            #activation_service.activation_hm(),
            #activation_service.activation_stellantis(),
            #activation_service.activation_tesla_particulier())

        # Get updated fleet info after activation
        logging.info('-------------------------------Activation completed-------------------------------')
        df = await fleet_info(owner_filter=owner_filter)
        
        # Process vehicles with updated info
        vehicle_processor = VehicleProcessor(
            bmw_api=bmw_api,
            hm_api=hm_api,
            stellantis_api=stellantis_api,
            tesla_api=tesla_api,
            tesla_particulier_api=tesla_particulier_api,
            renault_api=renault_api,
            df=df
        )
        await asyncio.gather(
            #vehicle_processor.process_other_vehicles(),
            #vehicle_processor.process_tesla(),
            ##vehicle_processor.process_renault(),
            #vehicle_processor.process_deactivated_vehicles(),
            vehicle_processor.process_bmw()
            #vehicle_processor.process_tesla_particulier())
        ##await vehicle_processor.delete_unused_models()
        ##await vehicle_processor.generate_vehicle_summary()

    except Exception as e:
        logger.error(f"Error processing vehicles: {str(e)}")
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
        logger.error(f"Error during cleanup: {str(e)}")
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
