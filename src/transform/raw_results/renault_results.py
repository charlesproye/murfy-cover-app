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
        .filter(lambda ts: ts["trimmed_in_discharge_idx"].max() >= 6)
        .pipe(charge_levels)
    )

def charge_levels(tss:DF) -> DF:
    tss_grp = tss.groupby("vin")
    return (
        tss
        .assign(soc_diff=tss_grp["soc"].diff())
        .eval("level_1 = soc_diff * (charging_rate < @LEVEL_1_MAX_POWER)")
        .eval("level_2 = soc_diff * (charging_rate.between(@LEVEL_1_MAX_POWER, @LEVEL_2_MAX_POWER))")
        .eval("level_3 = soc_diff * (charging_rate > @LEVEL_2_MAX_POWER)")
    )


if __name__ == "__main__":
    df = get_results().dropna(subset=["odometer", "soh"])
    print(df)
    if not df.empty:
        fig = px.scatter(df, x="date", y="soh", color="vin")
        fig.show()

