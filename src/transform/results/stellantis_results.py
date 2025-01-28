from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from core.console_utils import main_decorator
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries
from transform.results.config import *

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
    if not df.empty:
        fig = px.line(df, x="date", y="soh", color="vin")
        fig.show()

def get_results() -> DF:
    logger.info("Getting Volvo results")
    return (
        ProcessedTimeSeries("stellantis")
        .eval("odometer = odometer.ffill().bfill()")
        .eval("soh_oem = soh_oem.ffill().bfill()")
        .query("soc > 0.7")
    )

if __name__ == "__main__":
    main()

