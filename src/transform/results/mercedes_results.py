from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.processed_tss.main import get_processed_tss
from transform.results.config import *

logger = getLogger("transform.results.mercedes_results")

@main_decorator
def main():
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    df = (
        get_results()
        .dropna(subset=["odometer", "soh"])
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
        fig = px.scatter(df, x="date", y="soh", color="vin")
        fig.show()

def get_results() -> DF:
    return (
        get_processed_tss("mercedes-benz")
        .groupby("vin")
        .apply(fill_vars, include_groups=False)
        .reset_index()
        .assign(charge_size = lambda df: df.groupby(["vin", "in_charge_idx"]).transform("size"))
        .query("soc > 0.7 & charge_size > 10 & ~in_discharge_perf_mask")
        .eval("soh = estimated_range / soc / range")
        .sort_values(["vin", "date"])
    )

def fill_vars(ts:DF) -> DF:
    return (
        ts
        .eval("soc = soc.ffill().bfill()")
        .eval("estimated_range = estimated_range.ffill().bfill()")
    )

if __name__ == "__main__":
    main()
