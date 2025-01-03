import logging
from datetime import datetime as DT
from datetime import timedelta as TD
import logging.config

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from transform.config import *
# from transform.fleet_info.main import update_db_vehicle_table
from transform.raw_tss.main import update_all_raw_tss
from transform.processed_tss.main import update_all_processed_tss
from core.console_utils import main_decorator, parse_kwargs
from core.logging_utils import set_level_of_loggers_with_prefix

@main_decorator
def main():
    cli_kwargs = parse_kwargs(MAIN_KWARGS)

    print(cli_kwargs)

    setup_logging(cli_kwargs["log_level"].upper())
    scheduler = BlockingScheduler()
    scheduler.add_job(
        func=run_entire_pipeline,
        name="data_ev_pipeline",
        id="data_ev_pipeline",
        trigger=IntervalTrigger(
            days=1,
            start_date=DT.now() + TD(seconds=3)
        ),
    )
    # Start
    scheduler.start()

def setup_logging(transform_loggers_level=logging.INFO):
    set_level_of_loggers_with_prefix(transform_loggers_level, "transform")
    logger = logging.getLogger("apscheduler.scheduler")
    logger.setLevel(logging.INFO)

def run_entire_pipeline():
    update_all_raw_tss()
    # update_db_vehicle_table()
    update_all_processed_tss()

if __name__ == '__main__':
    main()

