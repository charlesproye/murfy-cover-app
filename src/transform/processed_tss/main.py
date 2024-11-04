from typing import Callable
import inspect
from logging import getLogger

from pandas import DataFrame as DF

from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from transform.processed_tss.high_mobility_processed_tss import get_processed_tss as hm_get_processed_tss
from transform.processed_tss.bmw_processed_tss import get_processed_tss as bmw_get_processed_tss

logger = getLogger("transform.processed_tss.main")

GET_PROCESSED_TSS_FUNCTIONS:dict[str, Callable[[], DF]] = {
    "bmw":              bmw_get_processed_tss,
    "kia":              hm_get_processed_tss,
    "mercedes-benz":    hm_get_processed_tss,
    "ford":             hm_get_processed_tss,
    "renault":          hm_get_processed_tss,
}

def get_processed_tss(brand:str, **kwargs) -> DF:
    func = GET_PROCESSED_TSS_FUNCTIONS[brand]
    if "brand" in inspect.signature(func).parameters:
        kwargs["brand"] = brand
    return func(**kwargs)

def update_all_processed_tss():
    #set_level_of_loggers_with_prefix("DEBUG", "transform.processed_tss")
    for brand in list(GET_PROCESSED_TSS_FUNCTIONS.keys()):
        logger.debug(f"================={brand}=================")
        single_dataframe_script_main(get_processed_tss, brand=brand, force_update=True)

if __name__ == "__main__":
    update_all_processed_tss()
