import inspect
from typing import Callable
from logging import getLogger

from rich.progress import track
from pandas import DataFrame as DF

from transform.processed_tss.config import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.processed_tss.bmw_processed_tss import get_processed_tss as bmw_get_processed_tss
from transform.processed_tss.high_mobility_processed_tss import get_processed_tss as hm_get_processed_tss
from transform.processed_tss.tesla_processed_tss import get_processed_tss as tesla_get_processed_tss

logger = getLogger("transform.processed_tss.main")

GET_PROCESSED_TSS_FUNCTIONS:dict[str, Callable[[], DF]] = {
    "bmw":             bmw_get_processed_tss,
    "kia":              hm_get_processed_tss,
    "mercedes-benz":    hm_get_processed_tss,
    "ford":             hm_get_processed_tss,
    "renault":          hm_get_processed_tss,
    "fiat":             hm_get_processed_tss,
    "opel":             hm_get_processed_tss,
    "ds":               hm_get_processed_tss,
    "peugeot":          hm_get_processed_tss,
    "volvo-cars":       hm_get_processed_tss,
    "tesla":            tesla_get_processed_tss,
}

def get_processed_tss(brand:str, **kwargs) -> DF:
    func = GET_PROCESSED_TSS_FUNCTIONS[brand]
    if "brand" in inspect.signature(func).parameters:
        kwargs["brand"] = brand
    return func(**kwargs)

def update_all_processed_tss():
    set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    for brand in track(list(GET_PROCESSED_TSS_FUNCTIONS.keys()), description="Updating processed TSSs..."):
        logger.debug(f"================={brand}=================")
        single_dataframe_script_main(get_processed_tss, logger=logger, brand=brand, force_update=True)

if __name__ == "__main__":
    update_all_processed_tss()

