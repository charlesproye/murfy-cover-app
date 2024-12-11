from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from transform.processed_tss.main import get_processed_tss
from transform.results.config import *

logger = getLogger("transform.results.renault_results")

def get_results() -> DF:
    return (
        get_processed_tss("renault")
        .eval("expected_battery_energy = capacity * soc")
        .eval("soh = battery_energy / expected_battery_energy") 
        .query("~in_discharge & soc > 0.5")
    )

if __name__ == "__main__":
    df = get_results().dropna(subset=["odometer", "soh"])
    print(df)
    if not df.empty:
        fig = px.scatter(df, x="date", y="soh", color="vin")
        fig.show()

