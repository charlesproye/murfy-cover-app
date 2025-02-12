import asyncio
import logging
import pandas as pd
import signal
import sys
from typing import Optional
from functools import partial

from ingestion.vehicle_info.config.settings import LOGGING_CONFIG, BMW_AUTH_URL, BMW_BASE_URL, BMW_CLIENT_ID, BMW_FLEET_ID, \
    BMW_CLIENT_USERNAME, BMW_CLIENT_PASSWORD, HM_BASE_URL, HM_CLIENT_ID, HM_CLIENT_SECRET, STELLANTIS_BASE_URL, STELLANTIS_EMAIL, STELLANTIS_PASSWORD, STELLANTIS_FLEET_ID, STELLANTIS_COMPANY_ID, SLACK_TOKEN
from ingestion.vehicle_info.api.bmw_client import BMWApi
from ingestion.vehicle_info.api.hm_client import HMApi
from ingestion.vehicle_info.api.stellantis_client import StellantisApi
from ingestion.vehicle_info.api.tesla_client import TeslaApi
from ingestion.vehicle_info.services.activation_service import VehicleActivationService
from ingestion.vehicle_info.services.vehicle_processor import VehicleProcessor
from ingestion.vehicle_info.fleet_info import read_fleet_info as fleet_info

# Configure logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)

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
        logger.info("All tasks cancelled. Exiting...")

async def get_existing_model_metadata():
    """Retrieve existing model metadata from the database."""
    from core.sql_utils import get_connection
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                m.make_name,
                vm.model_name,
                vm.type,
                vm.url_image,
                vm.warranty_km,
                vm.warranty_date,
                vm.capacity
            FROM vehicle_model vm
            JOIN make m ON vm.make_id = m.id
            WHERE vm.url_image IS NOT NULL 
               OR vm.warranty_km IS NOT NULL 
               OR vm.warranty_date IS NOT NULL 
               OR vm.capacity IS NOT NULL
            ORDER BY m.make_name, vm.model_name, vm.type
        """)
        
        results = cursor.fetchall()
        
        if results:
            print("\nExisting model metadata:")
            print("--------------------------------------------------------------------------------")
            print("Make | Model | Type | URL | Warranty km | Warranty years | Capacity")
            print("--------------------------------------------------------------------------------")
            for row in results:
                make, model, type_value, url, warranty_km, warranty_date, capacity = row
                print(f"{make} | {model} | {type_value or 'N/A'} | {url or 'N/A'} | {warranty_km or 'N/A'} | {warranty_date or 'N/A'} | {capacity or 'N/A'}")
            print("--------------------------------------------------------------------------------")
        else:
            print("No metadata found in database")
            
        return results

async def main(owner_filter: Optional[str] = None):
    """Main entry point for vehicle processing."""
    try:
        logger.info("Starting vehicle processing...")
        
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
        
        # Initialize Tesla API with required parameters
        tesla_api = TeslaApi(
            base_url="https://fleet-api.prd.eu.vn.cloud.tesla.com",
            slack_token=SLACK_TOKEN,
            slack_channel_id="C0816LXFCNL"  # Channel ID from tesla.py
        )
        
        df = await fleet_info(owner_filter=owner_filter)
        logger.info(f"Total vehicles in fleet_info: {len(df)}")
                
        activation_service = VehicleActivationService(bmw_api, hm_api, stellantis_api, tesla_api)
        activation_service.set_fleet_info(df)
        
        vehicle_processor = VehicleProcessor(activation_service)
        
        await vehicle_processor.process_vehicles(df)
        
        # Optionally get model metadata
        # await get_existing_model_metadata()
        
    except asyncio.CancelledError:
        logger.info("Processing cancelled by user.")
        raise
    except Exception as e:
        logger.error(f"Error in main program: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Create the main task
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(main(owner_filter="Ayvens"))
        
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
