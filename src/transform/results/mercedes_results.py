from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from core.constants import KWH_TO_KJ
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
        .assign(discharge_size = lambda df: df.groupby(["vin", "in_discharge_idx"]).transform("size"))
        .query("soc.between(0.7, 0.98) & discharge_size > 10")
        .groupby('model', group_keys=False)
        .apply(apply_model_calculation)
        .sort_values(["vin", "date"])
        .pipe(update_in_charge_idx)
        .pipe(compute_charging_rate)
        .pipe(charge_levels)
    )

def charge_levels(tss:DF) -> DF:
    tss_grp = tss.groupby("vin")
    return (
        tss
        .assign(soc_diff=tss_grp["soc"].diff())
        .eval("level_1 = soc_diff * (power < @LEVEL_1_MAX_POWER)")
        .eval("level_2 = soc_diff * (power.between(@LEVEL_1_MAX_POWER, @LEVEL_2_MAX_POWER))")
        .eval("level_3 = soc_diff * (power > @LEVEL_2_MAX_POWER)")
    )

def fill_vars(tss:DF, cols:list[str]) -> DF:
    tss_grouped = tss.groupby("vin")
    for col in cols:
        tss[col] = tss_grouped[col].ffill()
        tss[col] = tss_grouped[col].bfill()
    return tss

def compute_charging_rate(tss:DF) -> DF:
    print("compute_charging_rate called")
    tss_grp = tss.groupby("vin")
    tss = (
        tss
        .assign(
            soc_diff=tss_grp["soc"].diff(),
            time_diff=tss_grp["date"].diff().dt.as_unit("s").astype(int),
        )
        .eval("power = capacity * @KWH_TO_KJ * soc_diff / time_diff")
        .eval("power = power.mask(time_diff > 3600, 0)")
    )
    tss["power"] = tss.groupby(["vin", "in_charge_idx"])["power"].transform("median")
    return tss

def update_in_charge_idx(tss:DF) -> DF:
    #tss_grp = tss.groupby("vin")
    tss = tss.dropna(subset=["soc", "date"])
    tss["in_new_charge"] = tss.groupby("vin")["in_charge"].shift(1, fill_value=False).ne(tss["in_charge"])
    tss["in_charge_idx"] = tss.groupby("vin")["in_new_charge"].cumsum()
    return tss



if __name__ == "__main__":
    main()
