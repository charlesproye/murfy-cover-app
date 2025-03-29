from logging import Logger, getLogger

import plotly.express as px
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

def get_results() -> DF:
    logger.info("Getting Stellantis results")
    return (
        ProcessedTimeSeries("stellantis")
        .eval("odometer = odometer.ffill().bfill()")
        .assign(soh=np.nan)
        # .eval("soh = soc.ffill().bfill()")
        # .query("soc > 0.7")
    )

if __name__ == "__main__":
    main()

