from logging import Logger, getLogger

import plotly.express as px
from core.stats_utils import estimate_cycles
import numpy as np
from core.pandas_utils import *
from core.console_utils import main_decorator
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries

logger = getLogger("transform.results.stellantis_results")

@main_decorator
def main():
    df = (
        get_results()
        .eval("date = date.dt.date")
        .groupby(["vin", "date"])
        .agg({
            "soh": "median",
            "odometer": "last",
        })
        .reset_index()
    )
    print(df)

def get_results() -> DF:
    logger.info("Getting Stellantis results")
    return (
        ProcessedTimeSeries("stellantis")
        .eval("odometer = odometer.ffill().bfill()")
        .assign(soh=np.nan)
        .eval('cycles = round(odometer / (range * soh))')
        # .eval("soh = soc.ffill().bfill()")
        # .query("soc > 0.7")
    )

if __name__ == "__main__":
    main()

