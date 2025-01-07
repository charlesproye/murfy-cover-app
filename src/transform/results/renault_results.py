from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries
from transform.results.config import *

logger = getLogger("transform.results.renault_results")

def get_results() -> DF:
    return (
        ProcessedTimeSeries("renault")
        .eval("expected_battery_energy = capacity * soc")
        .eval("soh = battery_energy / expected_battery_energy") 
        .query("~in_discharge & soc > 0.5")
        .groupby("vin")
        # Ensure that there are at least 3 discharge period
        # Since discharge_perf_idx is declared as discharge_perf_mask.diff().cumsum(), it increases per discharge AND charge, i.e 2 per discharge
        # So we check that the max is superiror or equal to 3 * 2
        .filter(lambda ts: ts["in_discharge_perf_idx"].max() >= 6)
    )

if __name__ == "__main__":
    df = get_results().dropna(subset=["odometer", "soh"])
    print(df)
    if not df.empty:
        fig = px.scatter(df, x="date", y="soh", color="vin")
        fig.show()

