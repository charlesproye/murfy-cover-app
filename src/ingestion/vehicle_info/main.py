import asyncio
import logging
import pandas as pd
import signal
import sys
from typing import Optional, List
from functools import partial
from dataclasses import dataclass

from ingestion.vehicle_info.config.settings import LOGGING_CONFIG
from ingestion.vehicle_info.config.credentials import *
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

@dataclass
class BrandOperations:
    """Data class to hold brand-specific operations."""
    name: str
    activation_task: callable
class VehicleProcessingManager:
    """Manages the vehicle processing operations for different brands."""
    
    def __init__(self, owner_filter: Optional[str] = None):
        self.owner_filter = owner_filter
        self.apis = self._initialize_apis()
        self.df = None
        self.activation_service = None
        self.vehicle_processor = None
    def _initialize_apis(self) -> dict:
        """Initialize all API clients."""
        return {
            'bmw': BMWApi(
                auth_url=BMW_AUTH_URL,
                base_url=BMW_BASE_URL,
                client_id=BMW_CLIENT_ID,
                fleet_id=BMW_FLEET_ID,
                client_username=BMW_CLIENT_USERNAME,
                client_password=BMW_CLIENT_PASSWORD
            ),
            'hm': HMApi(
                base_url=HM_BASE_URL,
                client_id=HM_CLIENT_ID,
                client_secret=HM_CLIENT_SECRET
            ),
            'stellantis': StellantisApi(
                base_url=STELLANTIS_BASE_URL,
                email=STELLANTIS_EMAIL,
                password=STELLANTIS_PASSWORD,
                fleet_id=STELLANTIS_FLEET_ID,
                company_id=STELLANTIS_COMPANY_ID
            ),
            'tesla': TeslaApi(
                base_url=TESLA_BASE_URL,
                slack_token=SLACK_TOKEN,
                slack_channel_id=SLACK_CHANNEL_ID
            )
        }
    
    async def initialize(self):
        """Initialize the processing manager with fleet data."""
        self.df = await fleet_info(owner_filter=self.owner_filter)
        logger.info(f"Total vehicles in fleet_info: {len(self.df)}")
        
        self.activation_service = VehicleActivationService(
            self.apis['bmw'], self.apis['hm'], 
            self.apis['stellantis'],
            self.apis['tesla'],
            self.df
        )
        #self.df = await fleet_info(owner_filter=self.owner_filter)
        #logger.info(f"Total vehicles in fleet_info: {len(self.df)}")
        #self.vehicle_processor = VehicleProcessor(
        #    self.apis['bmw'], self.apis['hm'], 
        #    self.apis['stellantis'], self.apis['tesla'],
        #    self.df
        #)
    
    async def process_brand(self, brand: str, activation_task: callable):
        """Process a single brand's operations."""
        try:
            logger.info(f"Starting {brand} operations...")
            await activation_task()
            #await processing_task()
            logger.info(f"Completed {brand} operations successfully")
        except Exception as e:
            logger.error(f"Error processing {brand}: {str(e)}")
            raise
    
    async def process_all_brands(self):
        """Process all brands in parallel."""
        brand_operations = [
            BrandOperations('tesla', self.activation_service.activation_tesla), #self.vehicle_processor.process_tesla),
            BrandOperations('bmw', self.activation_service.activation_bmw), #self.vehicle_processor.process_bmw),
            BrandOperations('hm', self.activation_service.activation_hm), #self.vehicle_processor.process_hm),
            BrandOperations('stellantis', self.activation_service.activation_stellantis) #self.vehicle_processor.process_stellantis)
        ]
        tasks = [
            self.process_brand(brand.name, brand.activation_task) #brand.processing_task)
            for brand in brand_operations
        ]
        
        await asyncio.gather(*tasks)

async def main(owner_filter: Optional[str] = None):
    """Main entry point for vehicle processing."""
    try:
        logger.info("Starting vehicle processing...")
        manager = VehicleProcessingManager(owner_filter)
        await manager.initialize()
        await manager.process_all_brands()
        
    except asyncio.CancelledError:
        logger.info("Processing cancelled by user.")
        raise
    except Exception as e:
        logger.error(f"Error in main program: {str(e)}")
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
        main_task = loop.create_task(main())
        
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
