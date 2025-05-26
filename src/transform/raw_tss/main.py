from typing import Callable
import inspect

from pandas import DataFrame as DF

from core.s3_utils import S3Service
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.raw_tss.high_mobility_raw_tss import get_raw_tss as hm_get_raw_tss
from transform.raw_tss.bmw_raw_tss import get_raw_tss as bmw_get_raw_tss
from transform.raw_tss.tesla_raw_tss import get_raw_tss as tesla_get_raw_tss
from transform.raw_tss.mobilisight_raw_tss import get_raw_tss as mobilisight_get_raw_tss
from transform.raw_tss.fleet_telemetry_raw_tss import get_raw_tss as fleet_telemetry_raw_tss

GET_RAW_TSS_FUNCTIONS:dict[str, Callable[[bool, S3Service], DF]] = {
    # Stellantis
    "stellantis":       mobilisight_get_raw_tss,
    # BMW
    "bmw":              bmw_get_raw_tss,
    # Tesla
    "tesla":            tesla_get_raw_tss,
    # Kia
    "kia":              hm_get_raw_tss,
    # Mercedes-Benz
    "mercedes-benz":    hm_get_raw_tss,
    # Ford
    "ford":             hm_get_raw_tss,
    # Renault
    "renault":          hm_get_raw_tss,
    # Volvo
    "volvo-cars":       hm_get_raw_tss,
    # fleet-telemetry
    "tesla-fleet-telemetry":  fleet_telemetry_raw_tss,
    
}

def get_raw_tss(brand:str, **kwargs) -> DF:
    func = GET_RAW_TSS_FUNCTIONS[brand]
    if "brand" in inspect.signature(func).parameters:
        kwargs["brand"] = brand
    
    return func(**kwargs)

def update_all_raw_tss():
    set_level_of_loggers_with_prefix("DEBUG", "transform")
    for brand in list(GET_RAW_TSS_FUNCTIONS.keys()):
        try:
            print(brand, ":")
            single_dataframe_script_main(get_raw_tss, brand=brand, force_update=True)
        except Exception as e:
            print(f"[red]Error updating {brand}:[/red] {e}")
            from rich.console import Console
            console = Console()
            console.print_exception(show_locals=False)
        print("============================")

if __name__ == "__main__":
    from rich.traceback import install
    install(extra_lines=1)
    update_all_raw_tss()
