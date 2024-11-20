from logging import getLogger

import uuid
from datetime import datetime as DT

from core.pandas_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.sql_utils import connection, upsert_table_with_df
from core.ev_models_info import models_info
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.ayvens_fleet_info import fleet_info as ayvens_fleet_info
from transform.fleet_info.tesla_fleet_info import test_tesla_fleet_info, followed_tesla_vehicles_info
from transform.fleet_info.config import RDB_TABLES_MERGE_KWARGS

logger = getLogger("transform.fleet_info")

def update_db_vehicle_table() -> DF:
    fleet_info = get_fleet_info_to_update_rdb_vehicle_table()
    upsert_table_with_df(fleet_info, "vehicle", "vin", logger=logger)
    
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
    for table_name, kwargs in RDB_TABLES_MERGE_KWARGS.items():
        db_table = pd.read_sql_table(table_name, connection)
        fleet_info = left_merge(fleet_info, db_table, **kwargs)
    fleet_info = fleet_info.dropna(subset=["vin"])
    # Generate new UUIDs for new entries
    uuids_lst = [str(uuid.uuid4()) for _ in range(len(fleet_info))]
    fleet_info["id"] = Series(uuids_lst, dtype="string")
    # Drop rows with missing essential data
    fleet_info = fleet_info.dropna(subset=["vin"])
    return fleet_info

def get_fleet_info() -> DF:
    return (
        pd.concat((ayvens_fleet_info, test_tesla_fleet_info))
        .eval("model = model.str.lower()")
        .eval("version = version.str.lower()")
        .pipe(set_all_str_cols_to_lower, but=["vin"])
        .pipe(left_merge, followed_tesla_vehicles_info, left_on=["vin"], right_on=["vin"], src_dest_cols=["model", "version"])
        .pipe(left_merge, models_info, left_on=["model", "version"], right_on=["model", "version"])
    )


if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.fleet_info")
    single_dataframe_script_main(update_db_vehicle_table)

fleet_info = get_fleet_info()
