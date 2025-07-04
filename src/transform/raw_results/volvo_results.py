from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from core.stats_utils import estimate_cycles
from core.caching_utils import cache_result
from core.console_utils import main_decorator
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries
from transform.raw_results.config import *

logger = getLogger("transform.raw_results.volvo_results")

@main_decorator
def main():
    df = (
        get_results(force_update=True)
        .eval("date = date.dt.date")
        .groupby(["vin", "date"])
        .agg({
            "soh": "median",
            "odometer": "last",
            "consumption": "mean",
        })
        .reset_index()
    )
    if not df.empty:
        fig = px.line(df, x="date", y="soh", color="vin")
        fig.show()

@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make="volvo-cars"), "s3")
def get_results() -> DF:
    logger.debug("Getting Volvo results")
    results = (
        ProcessedTimeSeries("volvo-cars")
        .eval("odometer = odometer.ffill().bfill()")
        .eval("soh = estimated_range / soc / range / 0.87")
        # .query("soc > 0.7")
    )
    # # logger.debug("Sanity check of the results:")
    # # logger.debug(sanity_check(results))
    results['cycles'] = results.apply(lambda x: estimate_cycles(x['odometer'], x['range'], x['soh']), axis=1)

    return results

if __name__ == "__main__":
    main()

