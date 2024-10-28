import logging
from datetime import datetime as DT
from datetime import timedelta as TD
import logging.config
import time

from pandas import Index
from pandas import Series
from pandas import DataFrame as DF
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from transform.config import *
from core.singleton_s3_bucket import bucket
from transform.fleet_info.ayvens_fleet_info import get_fleet_info
from transform.raw_tss.raw_tss import update_all_raw_tss
from core.console_utils import main_decorator, parse_kwargs
from core.logging_utils import set_level_of_loggers_with_prefix

from core.console_utils import single_dataframe_script_main

@main_decorator
def main():
    cli_kwargs = parse_kwargs(MAIN_KWARGS)

    print(cli_kwargs)

    # Setup logging
    setup_logging(cli_kwargs["log_level"].upper())
    # Setup pipelines
    # Setup scheduler
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
    scheduler.add_job(
        func=record_time_to_get_ayvens_fleet_info,
        name="data_ev_pipeline",
        id="data_ev_pipeline",
        trigger=IntervalTrigger(
            minutes=10,
            start_date=DT.now() + TD(seconds=3)
        ),
    )
    # Start
    scheduler.start()

def setup_logging(transform_loggers_level=logging.INFO):
    set_level_of_loggers_with_prefix(transform_loggers_level, "transform")
    logger = logging.getLogger("apscheduler.scheduler")
    logger.setLevel(logging.INFO)

def record_time_to_get_ayvens_fleet_info():
    PATH_TO_PARQUET_FLEET_INFO = "fleet_info/test_ayvens_fleet_info.parquet"
    PATH_TO_DURATION_DF = "fleet_info/durations.parquet"
    fleet_info = get_fleet_info()
    bucket.save_df_as_parquet(fleet_info, PATH_TO_PARQUET_FLEET_INFO)
    before = time.time()
    fleet_info = bucket.read_parquet_df(PATH_TO_PARQUET_FLEET_INFO)
    after = time.time()
    duration = after - before
    DEFAULT_DURATION_DF = DF(columns=["duration", "date"])
    durations_df = DEFAULT_DURATION_DF if not bucket.check_file_exists(PATH_TO_DURATION_DF) else bucket.read_parquet_df(PATH_TO_DURATION_DF)
    durations_df.loc[len(durations_df)] = Series({"duration": duration, "date": DT.now()})
    bucket.save_df_as_parquet(durations_df, PATH_TO_DURATION_DF)

    return durations_df


def run_entire_pipeline():
    update_all_raw_tss()

if __name__ == '__main__':
    # main()
    single_dataframe_script_main(record_time_to_get_ayvens_fleet_info)

