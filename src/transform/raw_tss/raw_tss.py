from typing import Callable
import inspect

from pandas import DataFrame as DF

from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.raw_tss.high_mobility_raw_tss import get_raw_tss as hm_get_raw_tss
from transform.raw_tss.bmw_raw_tss import get_direct_bmw_raw_tss as bmw_get_raw_tss
from transform.raw_tss.stellantis_raw_tss import get_raw_tss as stellantis_get_raw_tss
from transform.raw_tss.tesla_raw_tss import get_raw_tss as tesla_get_raw_tss


GET_RAW_TSS_FUNCTIONS:dict[str, Callable[[bool, S3_Bucket], DF]] = {
    "BMW":              bmw_get_raw_tss,
    "tesla":            tesla_get_raw_tss,
    "kia":              hm_get_raw_tss,
    "mercedes-benz":    hm_get_raw_tss,
    "ford":             hm_get_raw_tss,
    "renault":          hm_get_raw_tss,
    "opel":             stellantis_get_raw_tss,
    "citroën":          stellantis_get_raw_tss,
    "peugeot":          stellantis_get_raw_tss,
    "ds":               stellantis_get_raw_tss,
    "fiat":             stellantis_get_raw_tss,
}

def get_raw_tss(brand:str, **kwargs) -> DF:
    func = GET_RAW_TSS_FUNCTIONS[brand]
    if "brand" in inspect.signature(func).parameters:
        kwargs["brand"] = brand
    
    return func(**kwargs)

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    for brand in list(GET_RAW_TSS_FUNCTIONS.keys()):
        print(brand, ":")
        single_dataframe_script_main(get_raw_tss, brand=brand, force_update=True)
        print("============================")
