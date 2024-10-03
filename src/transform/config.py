from typing import Callable

from analysis.bmw.bmw_raw_tss import get_raw_tss as bmw_get_raw_tss
from analysis.stellantis.stellantis_raw_tss import get_raw_tss as stellantis_get_raw_tss
from analysis.high_mobility.high_mobility_raw_tss import get_raw_tss as hm_get_raw_tss


BRAND_PIPELINES:dict[str, list[Callable]] = {
    "bmw": [bmw_get_raw_tss],
    "stellantis": [stellantis_get_raw_tss],
    "kia": [hm_get_raw_tss],
}
