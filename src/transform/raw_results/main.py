from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.raw_results.odometer_aggregation import agg_last_odometer
from transform.raw_results.ford_results import get_results as ford_results
from transform.raw_results.tesla_results import get_results as tesla_results
from transform.raw_results.volvo_results import get_results as volvo_results
from transform.raw_results.renault_results import get_results as renault_results
from transform.raw_results.mercedes_results import get_results as mercedes_results
from transform.raw_results.config import *

def update_all_raw_tss():
    for make in MAKES_WITHOUT_SOH:
        agg_last_odometer(make, force_update=True)
    ford_results(force_update=True)
    mercedes_results(force_update=True)
    renault_results(force_update=True)
    tesla_results(force_update=True)
    volvo_results(force_update=True)

@main_decorator
def main():
    print("Updating all raw results...")
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_results")
    update_all_raw_tss()

if __name__ == "__main__":
    main()

