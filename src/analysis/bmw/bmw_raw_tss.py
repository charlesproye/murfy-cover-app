from glob import glob
import logging

import pandas as pd
from pandas import DataFrame as DF

from core.console_utils import main_decorator
from core.caching_utils import singleton_data_caching

from test import test

logger = logging.getLogger(__name__)



def get_raw_tss(api_response: dict) -> DF:
    raw_tss = DF.from_dict(api_response["data"], ).astype({"unit": "string", "key": "string", })
    unit_not_none = raw_tss["unit"].notna()
    # units_with_underscore = 
    # raw_tss.loc[unit_not_none, "key"] += "_" + raw_tss.loc[unit_not_none, "unit"]
    return (
        raw_tss
        .pipe(pd.pivot_table, columns="key", values="value", index="date_of_value", aggfunc="first")
    )


if __name__ == "__main__":
    print(get_raw_tss(test))
