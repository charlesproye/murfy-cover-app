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

    # Setup logging
    logging.config.dictConfig({
        'version': 1,
        "loggers": {
            "transform": {
                "level": cli_kwargs["log_level"].upper(),
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

    # Setup pipelines
    pipelines = DF.from_dict(BRAND_PIPELINES, orient="index")
    if "pipelines" in cli_kwargs:
        if isinstance(cli_kwargs["pipelines"], str):
            cli_kwargs["pipelines"] = [cli_kwargs["pipelines"]]
        pipelines:DF = pipelines.loc[cli_kwargs["pipelines"]]
    if "steps" in cli_kwargs:
        if isinstance(cli_kwargs["steps"], str):
            cli_kwargs["steps"] = [cli_kwargs["steps"]]
        pipelines:DF = pipelines.loc[:, cli_kwargs["steps"]]
    
    # Setup scheduler
    scheduler = BlockingScheduler()
    for brand, pipeline in pipelines.iterrows():
        scheduler.add_job(
            func=lambda : [func(brand=brand, force_update=True) for task, func in pipeline.dropna().items()],
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

