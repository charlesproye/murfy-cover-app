import os
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime as DT
from datetime import timedelta as TD
import argparse
import logging.config

from apscheduler.triggers.interval import IntervalTrigger

from utils.platform import PLATFORM_COLORED, PLATFORM
from bib_models.utils.log_format import get_handler
from transform.config import *
from core.console_utils import main_decorator

@main_decorator
def main():
    # Set up logging
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO", help="Set the logging level (e.g., DEBUG, INFO)")
    args = parser.parse_args()

    logging.config.dictConfig({
        'version': 1,
        "loggers": {
            "transform": {
                "level": args.log_level.upper(),
                'handlers': ['console'],
                'propagate': False,
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
        },
        'formatters': {
            'default': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },
    })


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

