from logging import getLogger

from core.pandas_utils import *
from core.sql_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.stats_utils import filter_results
from transform.results.tesla_results import get_results
from transform.results.config import *

logger = getLogger("transform.results.main")

def update_vehicle_data_table():
    logger.info("Updating 'vehicle_data' table.")
    results = (
        get_results()
        .assign(date=lambda df: df["date"].dt.floor(UPDATE_FREQUENCY))
        .groupby(["vin", "date"])
        .agg({
            "odometer": "last",    
            "soh": "median",
        })
        .reset_index(drop=False)
        .pipe(filter_results, VALID_SOH_POINTS)
        .pipe(left_merge_rdb_table, "vehicle", left_on="vin", right_on="vin", src_dest_cols={"id": "vehicle_id"})
        .pipe(
            right_union_merge_rdb_table,
            "vehicle_data",
            left_on=["vehicle_id", "date"],
            right_on=["vehicle_id", "timestamp"],
            src_dest_cols=["soh", "odometer"]
        )
    )

    return results


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(update_vehicle_data_table)

