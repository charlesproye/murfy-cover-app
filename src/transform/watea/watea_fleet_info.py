from pandas import DataFrame as DF

from core.console_utils import single_dataframe_script_main
from core.caching_utils import cache_result
from transform.watea.watea_processed_tss import get_processed_tss
from transform.watea.watea_config import *

@cache_result(FLEET_INFO_DF_PATH, on="local_storage")
def get_fleet_info() -> DF:
    tss = (
        get_processed_tss()
        .set_index("id")
    )
    fleet_info = {
        "max_odo": tss.groupby("id")["odometer"].max(),
        "min_odo": tss.groupby("id")["odometer"].min(),
    }
    for period in ["in_charge", "in_discharge"]:
        period_tss_grpby = tss.query(period).groupby(level=0)
        for col in COLS_TO_DESCRIBE_IN_FLEET_INFO:
            col_in_period = period_tss_grpby[col]
            fleet_info[f"{period}_{col}_notna_ratio"] = col_in_period.count().div(col_in_period.size(), fill_value=0.0)

    fleet_info = (
        DF(fleet_info)
        .reset_index(drop=False)
        .eval("has_power_during_charge = in_charge_power_notna_ratio > 0.5")
        .eval("has_power_during_discharge = in_discharge_power_notna_ratio > 0.5")
    )

    return fleet_info
    
if __name__ == "__main__":
    single_dataframe_script_main(
        get_fleet_info,
        force_update=True,
    )

fleet_info = get_fleet_info()
