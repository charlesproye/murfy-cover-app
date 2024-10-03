import os
import logging
from datetime import datetime as DT
from datetime import timedelta as TD
import logging.config

from pandas import DataFrame as DF
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from transform.config import *
from core.console_utils import main_decorator, parse_kwargs

@main_decorator
def main():
    cli_kwargs = parse_kwargs(MAIN_KWARGS)

    print(cli_kwargs)

    # Setup logging
    setup_logging(cli_kwargs["log_level"].upper())
    # Setup pipelines
    pipelines = get_pipelines_to_run(cli_kwargs)
    # Setup scheduler
    scheduler = BlockingScheduler()
    for brand, pipeline in pipelines.iterrows():
        scheduler.add_job(
        func=lambda brand=brand, pipeline=pipeline: [func(brand=brand, force_update=True) for task, func in pipeline.dropna().items()],
            name=brand,
            id=brand,
            trigger=IntervalTrigger(
                days=1,
                start_date=DT.now() + TD(seconds=3)
            ),
        )
    # Start
    scheduler.start()

def setup_logging(transform_loggers_level=logging.INFO):
    logging.config.dictConfig({
        'version': 1,
        "loggers": {
            "transform": {
                "level": transform_loggers_level,
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
    logging.info(f"Main process PID: {os.getpid()}")
    
def get_pipelines_to_run(cli_kwargs:dict) -> DF:
    pipelines = DF.from_dict(BRAND_PIPELINES, orient="index")
    if "pipelines" in cli_kwargs:
        if isinstance(cli_kwargs["pipelines"], str):
            cli_kwargs["pipelines"] = [cli_kwargs["pipelines"]]
        pipelines:DF = pipelines.loc[cli_kwargs["pipelines"]]
    if "steps" in cli_kwargs:
        if isinstance(cli_kwargs["steps"], str):
            cli_kwargs["steps"] = [cli_kwargs["steps"]]
        pipelines:DF = pipelines.loc[:, cli_kwargs["steps"]]

    return pipelines

if __name__ == '__main__':
    main()

