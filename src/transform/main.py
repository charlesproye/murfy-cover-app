import os
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime as DT
from datetime import timedelta as TD

from apscheduler.triggers.interval import IntervalTrigger

from utils.platform import PLATFORM_COLORED, PLATFORM
from bib_models.utils.log_format import get_handler
from transform.config import *

handler = get_handler(is_logged=os.getenv("IS_DEPLOY",False), platform=PLATFORM)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    handlers=[handler]
)

def main():
    # Set up logger
    logger = logging.getLogger("apscheduler.scheduler")
    logger.setLevel(logging.INFO)
    logging.info(f"Main process PID: {os.getpid()}, running on {PLATFORM_COLORED}")
    # Set up scheduler
    scheduler = BlockingScheduler()
    for brand, pipeline in BRAND_PIPELINES.items():
        scheduler.add_job(
            func=lambda : [func(brand=brand, force_update=True) for func in pipeline],
            name=brand,
            id=brand,
            trigger=IntervalTrigger(
                days=1,
                start_date=DT.now() - TD(days=1) + TD(seconds=1)
            ),
        )
    # Start
    scheduler.start()


if __name__ == '__main__':
    main()

