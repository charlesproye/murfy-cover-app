from logging import getLogger

from core.pandas_utils import *
from core.sql_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import single_dataframe_script_main
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
        .pipe(filter_results)
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

def filter_results(results: DF) -> DF:
    logger.debug("Filtering results.")
    max_intercept, max_slope = intercept_and_slope_from_points(VALID_SOH_POINTS.xs("max", level=0, drop_level=True))
    print(f"max_intercept: {max_intercept}, max_slope: {max_slope}")
    min_intercept, min_slope = intercept_and_slope_from_points(VALID_SOH_POINTS.xs("min", level=0, drop_level=True))
    return (
        results
        .eval(f"max_valid_soh = odometer * {max_slope:f} + {max_intercept:f}")
        .eval(f"min_valid_soh = odometer * {min_slope:f} + {min_intercept:f}")
        .eval(f"soh_is_valid = soh <= max_valid_soh & soh >= min_valid_soh & soh > 0.5 & soh < 1.0")
        .pipe(debug_df, subset=["soh", "max_valid_soh", "min_valid_soh", "soh_is_valid"], logger=logger)
        .query("soh_is_valid")
        .dropna(subset=["soh", "odometer"], how="any")
    )

def intercept_and_slope_from_points(points: DF) -> tuple[float, float]:
    logger.debug(f"points:\n{points}")
    slope = (points.at["B", "soh"] - points.at["A", "soh"]) / (points.at["B", "odometer"] - points.at["A", "odometer"])
    intercept = points.at["A", "soh"] - slope * points.at["A", "odometer"]
    return intercept, slope

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(update_vehicle_data_table)

