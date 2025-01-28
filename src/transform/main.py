from logging import getLogger
from datetime import datetime as DT
from datetime import timedelta as TD
import logging.config
import schedule 
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from core.console_utils import main_decorator, parse_kwargs
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.config import *

from transform.raw_tss.main import update_all_raw_tss
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries
from transform.results.main import fill_vehicle_data_table_with_results
from transform.vehicle_info.main import VehicleInfoProcessor
from transform.front_utils.main import FrontUtils


logger = getLogger("transform.main")


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
    try:
        logging.info("Starting pipeline execution")
        start_time = DT.now()

        VehicleInfoProcessor().process_all_vehicles()
        logging.info("Vehicle info update completed")
        
        # update_all_raw_tss()
        # logging.info("Raw TSS update completed")
        
        # ProcessedTimeSeries.update_all_tss()
        # logging.info("Processed TSS update completed")

        # fill_vehicle_data_table_with_results()
        # logging.info("Results update completed")

        FrontUtils().update_scoring()
        logging.info("SOH comparison update completed")
        
        end_time = DT.now()
        duration = end_time - start_time
        logging.info(f"Pipeline completed successfully in {duration}")
        
    except Exception as e:
        raise

def run_scheduler():
    # Programmer l'exécution tous les jours à minuit
    logger.info("Scheduling pipeline execution")
    # schedule.every().day.at("11:26").do(run_entire_pipeline)
    ## For testing
    run_entire_pipeline()
    logger.info("Scheduler started - Pipeline will run daily at midnight")
    
    # Boucle infinie pour maintenir le scheduler actif
    while True:
        schedule.run_pending()

if __name__ == "__main__":
    logger.info("Starting transform pipeline")
    run_scheduler()

