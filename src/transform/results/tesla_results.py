from logging import getLogger

import plotly.express as px

from core.pandas_utils import *
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from core.stats_utils import lr_params_as_series
from transform.results.config import *
from transform.processed_tss.ProcessedTimeSeries import TeslaProcessedTimeSeries


logger = getLogger("transform.results.tesla_results")

@main_decorator
def main():
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    df = get_results()
    df.to_csv("tesla_results.csv")
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
    if not df.empty:
        fig = px.scatter(df, x="odometer", y="soh", color="vin")
        fig.show()

USE_COLS = [
    "vin",
    "trimmed_in_charge_idx",
    "trimmed_in_charge",
    "charge_energy_added",
    "soc",
    "inside_temp",
    "capacity",
    "odometer",
    "model",
    "date",
    "tesla_code",
    "battery_heater",
    "charging_power",
    "version",
]

def get_results() -> DF:
    charges:DF = (
        TeslaProcessedTimeSeries("tesla", use_cols=USE_COLS)
        .query("trimmed_in_charge")
        .groupby(["vin", "trimmed_in_charge_idx"])
        .agg(
            energy_added=pd.NamedAgg("charge_energy_added", series_start_end_diff),
            soc_diff=pd.NamedAgg("soc", series_start_end_diff),
            inside_temp=pd.NamedAgg("inside_temp", "mean"),
            capacity=pd.NamedAgg("capacity", "first"),
            odometer=pd.NamedAgg("odometer", "first"),
            version=pd.NamedAgg("version", "first"),
            size=pd.NamedAgg("soc", "size"),
            model=pd.NamedAgg("model", "first"),
            date=pd.NamedAgg("date", "first"),
            charging_power=pd.NamedAgg("charging_power", "median"),
            tesla_code=pd.NamedAgg("tesla_code", "first"),
        )
        .reset_index(drop=False)
        .eval("soh = energy_added / (soc_diff / 100.0 * capacity)")
        .query("soc_diff > 20 & soh.between(0.75, 1.0)")
    )
    mean_soh = charges["soh"].mean()
    inside_temp_soh_lr = lr_params_as_series(charges, "inside_temp", "soh")
    inside_temp_soh_lr
    return (
        charges
        .eval("soh_offset_pred = inside_temp * @inside_temp_soh_lr['slope'] + @inside_temp_soh_lr['intercept']")
        .eval("soh = soh - soh_offset_pred + @mean_soh")
        .eval("level_1 = soc_diff * (charging_power < @LEVEL_1_MAX_POWER) / 100")
        .eval("level_2 = soc_diff * (charging_power.between(@LEVEL_1_MAX_POWER, @LEVEL_2_MAX_POWER)) / 100")
        .eval("level_3 = soc_diff * (charging_power > @LEVEL_2_MAX_POWER) / 100")

    )

if __name__ == "__main__":
    main()

