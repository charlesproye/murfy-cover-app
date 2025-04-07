"""
This script is used to aggregate the odometer data for each vehicle of a brand that we can't calculate the SOH for.
"""
import plotly.express as px
import numpy as np

from core.pandas_utils import *
from core.console_utils import main_decorator
from core.caching_utils import cache_result
from transform.raw_results.config import *
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries as TSS

@main_decorator
def main():
    for make in MAKES_WITHOUT_SOH:
        results = agg_last_odometer(make, force_update=True)
        ids_to_plot = results["vin"].value_counts(sort=True, ascending=False).index[:4]
        df_to_plot = results[results["vin"].isin(ids_to_plot)]
        px.line(df_to_plot, x="date", y="odometer", color="vin", title=make).show()

@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE, "s3", ["make"])
def agg_last_odometer(make:str) -> DF:
    from transform.processed_results.config import UPDATE_FREQUENCY
    logger.info(f"Processing odometer aggregation of {make}.")
    tss = TSS(make)
    return (
        tss
        .eval("floored_date = date.dt.floor(@UPDATE_FREQUENCY)")
        .groupby([tss.id_col, "floored_date"], observed=True)
        .agg({
            "odometer": "last", 
            "make": "first",
            "model": "first",
            "version": "first",
        })
        .reset_index()
        .rename(columns={"floored_date": "date"})
        .dropna(subset=["odometer"])
        .assign(soh=np.nan)
        .astype({"soh": "float"})
    )

if __name__ == "__main__":
    main()

