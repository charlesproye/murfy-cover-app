from logging import getLogger

import plotly.express as px

from core.pandas_utils import *
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.results.config import *
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries

logger = getLogger("transform.results.tesla_results")

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
    if not df.empty:
        fig = px.scatter(df, x="odometer", y="soh", color="vin")
        fig.show()

def get_results() -> DF:
    return (
        ProcessedTimeSeries("tesla")
        .query("trimmed_in_charge")
        .groupby(["vin", "trimmed_in_charge_idx"])
        .agg(
            energy_added=pd.NamedAgg("charge_energy_added", series_start_end_diff),
            soc_diff=pd.NamedAgg("soc", series_start_end_diff),
            soc_start=pd.NamedAgg("soc", "first"),
            soc_end=pd.NamedAgg("soc", "last"),
            temp=pd.NamedAgg("inside_temp", "mean"),
            capacity=pd.NamedAgg("capacity", "first"),
            odometer=pd.NamedAgg("odometer", "first"),
            fast_charger_type=pd.NamedAgg("fast_charger_type", Series.mode),
            size=pd.NamedAgg("soc", "size"),
            model=pd.NamedAgg("model", "first"),
            version=pd.NamedAgg("version", "first"),
            date=pd.NamedAgg("date", "first"),
        )
        .reset_index(drop=False)
        .eval("soh = energy_added / (soc_diff / 100 * capacity)")
        .eval("model_version = model + version")
        .eval("level_1 = charger_power < @LEVEL_1_MAX_POWER")
        .eval("level_2 = charger_power.between(@LEVEL_1_MAX_POWER, @LEVEL_2_MAX_POWER)")
        .eval("level_3 = charger_power > @LEVEL_2_MAX_POWER")
        .query("soc_diff > 20")
    )

if __name__ == "__main__":
    main()

