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
    df = get_results()
    print(df)
    # print(df["soh"].count())
    df = (
        df
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

def apply_model_calculation(group:DF) -> DF:
    model = group.name
    calculation = model_calculations.get(model, model_calculations['default'])
    group['soh'] = calculation(group)
    # print(group.shape)
    return group

def get_results() -> DF:
    return (
        ProcessedTimeSeries("mercedes-benz")
        .pipe(fill_vars, cols=["soc", "estimated_range", "range"])
        .reset_index()
        .assign(discharge_size = lambda df: df.groupby(["vin", "in_discharge_idx"]).transform("size"))
        .query("soc > 0.7 & soc < 0.98 & discharge_size > 10") # & in_discharge")
        .groupby('model', group_keys=False)
        .apply(apply_model_calculation)
        .sort_values(["vin", "date"])
    )

def fill_vars(tss:DF, cols:list[str]) -> DF:
    tss_grouped = tss.groupby("vin")
    for col in cols:
        tss[col] = tss_grouped[col].ffill()
        tss[col] = tss_grouped[col].bfill()
    return tss

if __name__ == "__main__":
    main()
