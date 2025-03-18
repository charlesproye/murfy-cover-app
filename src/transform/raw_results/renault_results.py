from logging import getLogger

import plotly.express as px

from core.pandas_utils import *
from transform.raw_results.config import *
from core.caching_utils import cache_result
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries


logger = getLogger("transform.raw_results.renault_results")

@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make="renault"), "s3")
def get_results() -> DF:
    logger.info("Processing raw renault results.")
    return (
        ProcessedTimeSeries("renault", filters=[("in_charge", "==", True)])
        .eval("expected_battery_energy = capacity * soc")
        .eval("soh = battery_energy / expected_battery_energy") 
        # .query("soc > 0.5")
        # .groupby("vin")
        # # Ensure that there are at least 3 discharge period
        # # Since discharge_perf_idx is declared as discharge_perf_mask.diff().cumsum(), it increases per discharge AND charge, i.e 2 per discharge
        # # So we check that the max is superiror or equal to 3 * 2
        # .filter(lambda ts: ts["trimmed_in_discharge_idx"].max() >= 6)
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
    df = get_results(force_update=True).dropna(subset=["odometer", "soh"])
    print(df)
    if not df.empty:
        fig = px.scatter(df, x="date", y="soh", color="vin")
        fig.show()

