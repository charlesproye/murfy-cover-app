from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries
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

model_calculations = {
    'vito': lambda df: df['estimated_range'] / df['soc'] / df['range'] / 0.97,
    'sprinter': lambda df: df['estimated_range'] / df['soc'] / df['range'] / 0.92,
    'default': lambda df: df['estimated_range'] / df['soc'] / df['range']
}

def apply_model_calculation(group):
    model = group['model'].iloc[0]
    calculation = model_calculations.get(model, model_calculations['default'])
    group['soh'] = calculation(group)
    return group

def get_results() -> DF:
    return (
        ProcessedTimeSeries("mercedes-benz")
        .groupby("vin")
        .apply(fill_vars, include_groups=False)
        .reset_index()
        .assign(discharge_size = lambda df: df.groupby(["vin", "in_discharge_idx"]).transform("size"))
        .query("soc > 0.7 & soc> 0.98&discharge_size > 10 & in_discharge_perf_mask")
        .groupby('model', group_keys=False)
        .apply(apply_model_calculation)
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
