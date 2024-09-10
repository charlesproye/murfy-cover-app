import asyncio
from asyncio.exceptions import CancelledError
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging

from rich import print

from jobs.high_mobility.high_mobility_raw_ts import HighMobilityRawTS
from jobs.high_mobility.high_mobility_processed_ts import HighMobilityProcessedTS
from utils.platform import PLATFORM_COLORED, PLATFORM
from bib_models.utils.log_format import get_handler
from jobs.high_mobility.constants import *

handler = get_handler(is_logged=os.getenv("IS_DEPLOY",False), platform=PLATFORM)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    handlers=[handler]
)

async def main(start_scheduler: bool = True):
    scheduler = AsyncIOScheduler()

    logger = logging.getLogger("apscheduler.scheduler")
    logger.setLevel(logging.WARNING)
    logging.info(f"Main process PID: {os.getpid()}, running on {PLATFORM_COLORED}")
    #### Daily

    for brand in HM_HANDLED_BRANDS:
        # await HighMobilityRawTS(brand).add_to_schedule(scheduler)
        await HighMobilityProcessedTS(brand).add_to_schedule(scheduler)

    # Start the scheduler
    if not start_scheduler:
        return
    scheduler.start()

    logger.setLevel(logging.INFO)
    try:
        # Keep the main thread alive
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit, CancelledError):
        scheduler.shutdown(wait=False)
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Exiting...")

