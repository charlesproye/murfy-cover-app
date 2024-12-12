from logging import Logger, getLogger

import plotly.express as px

from core.pandas_utils import *
from core.console_utils import main_decorator
from transform.processed_tss.main import get_processed_tss
from transform.results.config import *

logger = getLogger("transform.results.volvo_results")

@main_decorator
def main():
    print(get_results())
    df = (
        get_results()
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
    print(get_processed_tss("volvo-cars"))
    print("olé")
    return (
        get_processed_tss("volvo-cars")
        .eval("soh = estimated_range / soc / range")
    )

if __name__ == "__main__":
    main()

