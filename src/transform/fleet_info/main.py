from logging import getLogger

from core.pandas_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.sql_utils import *
from core.ev_models_info import models_info
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.ayvens_fleet_info import fleet_info as ayvens_fleet_info
from transform.fleet_info.tesla_fleet_info import test_tesla_fleet_info, followed_tesla_vehicles_info
from transform.fleet_info.config import *

logger = getLogger("transform.fleet_info")

def update_db_vehicle_table() -> DF:
    fleet_info = get_fleet_info_to_update_rdb_vehicle_table()
    right_union_merge_rdb_table(
        fleet_info, 
        "vehicle", 
        left_on="vin", 
        right_on="vin", 
        src_dest_cols=RIGHT_MERGES_RDB_TABLES_FLEET_INFO_DEST_COLS
    )
    
    return fleet_info

def get_fleet_info_to_update_rdb_vehicle_table() -> DF:
    fleet_info = get_fleet_info()
    # Convert date columns to the correct format
    if 'end_of_contract_date' in fleet_info.columns:
        fleet_info['end_of_contract_date'] = pd.to_datetime(
            fleet_info['end_of_contract_date'], 
            format='%d-%m-%Y', 
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')
    # Merge with other tables to get required foreign keys
    for kwargs in LEFT_MERGES_RDB_TABLES_FLEET_INFO_KWARGS:
        fleet_info = left_merge_rdb_table(fleet_info, **kwargs)
    # Drop rows with missing essential data
    fleet_info = fleet_info.dropna(subset=["vin"])
    return fleet_info

def get_fleet_info() -> DF:
    return (
        pd.concat((ayvens_fleet_info, test_tesla_fleet_info))
        .pipe(set_all_str_cols_to_lower, but=["vin"])
        .pipe(left_merge, followed_tesla_vehicles_info, left_on=["vin"], right_on=["vin"], src_dest_cols=["model", "version"])
        .pipe(left_merge, models_info, left_on=["model", "version"], right_on=["model", "version"])
        .assign(version=lambda df: df["version"].mask(df["version"] == "gt première ev 50kwh 136 7,4kw 5d", "gt première ev 50kwh 136 7;4kw 5d"))
    )


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "core.sql_utils")
    set_level_of_loggers_with_prefix("DEBUG", "transform.fleet_info")
    single_dataframe_script_main(update_db_vehicle_table)

fleet_info = get_fleet_info()
