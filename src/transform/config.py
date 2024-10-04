from typing import Callable

from pandas import DataFrame as DF

from transform.bmw.bmw_raw_tss import get_raw_tss as bmw_get_raw_tss
from transform.stellantis.stellantis_raw_tss import get_raw_tss as stellantis_get_raw_tss
from transform.high_mobility.high_mobility_raw_tss import get_raw_tss as hm_get_raw_tss


BRAND_PIPELINES_DICT:dict[str, dict[str, Callable]] = {
    "BMW": {
        "raw_tss":bmw_get_raw_tss,
    },
    "kia":{
        "raw_tss":hm_get_raw_tss,
    },
    "mercedes-benz":{
        "raw_tss":hm_get_raw_tss,
    },
    "ford":{
        "raw_tss":hm_get_raw_tss,
    },
    # stellantis
    'opel': {
        "raw_tss": stellantis_get_raw_tss,
    },
    'citroën': {
        "raw_tss": stellantis_get_raw_tss,
    },
    'peugeot': {
        "raw_tss": stellantis_get_raw_tss,
    },
    "renault":{
        "raw_tss":hm_get_raw_tss,
    },
    'ds': {
        "raw_tss": stellantis_get_raw_tss,
    },
    'fiat': {
        "raw_tss": stellantis_get_raw_tss,
    },
}

BRAND_PIPELINES = DF.from_dict(BRAND_PIPELINES_DICT, orient="index")

MAIN_KWARGS = {
    "--log-level": {
        "default": "INFO",
        "type": str,
        "help": "Set the logging level (e.g., DEBUG, INFO)",
    },
    "--pipelines": {
        "required": False,
        "help": "Specifies pipeline or list of pipelines to run."
    },
    "--steps": {
        "required": False,
        "help": "Specifies pipeline step or list of pipelines steps to run."
    },
    "--print_pipelines": {
        "default": False,
        "help": "Flag to print the pipelines that will be executed."
    },
}

