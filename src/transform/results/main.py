from logging import getLogger

from core.pandas_utils import *
from core.sql_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
from core.stats_utils import filter_results
from transform.results.tesla_results import get_results as get_tesla_results
from transform.results.ford_results import get_results as get_ford_results
from transform.results.config import *

logger = getLogger("transform.results.main")
GET_RESULTS_FUNCS = [get_ford_results, get_tesla_results]

def update_vehicle_data_table():
    logger.info("Updating 'vehicle_data' table.")
    return (
        get_all_processed_results()
        .pipe(left_merge_rdb_table, "vehicle", "vin", "vin", {"id": "vehicle_id"})
        .pipe(
            right_union_merge_rdb_table,
            "vehicle_data",
            left_on=["vehicle_id", "date"],
            right_on=["vehicle_id", "timestamp"],
            src_dest_cols=["soh", "odometer"]
        )
    )

def get_all_processed_results() -> DF:
    def process_results(df: DF) -> DF:
        return (
            df
            .assign(date=lambda df: df["date"].dt.floor(UPDATE_FREQUENCY))
            .groupby(["vin", "date"])
            .agg({
                "odometer": "last",
                "soh": "median",
            })
            .reset_index(drop=False)
            .pipe(filter_results, VALID_SOH_POINTS, logger)
        )
    
    return pd.concat([process_results(get_results_func()) for get_results_func in GET_RESULTS_FUNCS])

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(update_vehicle_data_table)

