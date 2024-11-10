from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

from core.pandas_utils import *
from core.sql_utils import connection
from core.ev_models_info import models_info
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.ayvens_fleet_info import fleet_info as ayvens_fleet_info
from transform.fleet_info.tesla_fleet_info import fleet_info as tesla_fleet_info
from transform.fleet_info.config import RDB_TABLES_MERGE_KWARGS

def get_fleet_info() -> DF:
    return (
        pd.concat((ayvens_fleet_info, tesla_fleet_info))
        .pipe(left_merge, models_info, left_on=["model", "version"], right_on=["model", "version"])
    )

def update_db_vehicle_table() -> DF:
    fleet_info = get_fleet_info()
    fleet_info['activation_status'] = (
        fleet_info['activation_status']
        .fillna("unknown")
        .astype("string")
        .eq("activated")
    )
    for table_name, kwargs in RDB_TABLES_MERGE_KWARGS.items():
        db_table = pd.read_sql_table(table_name, connection)
        fleet_info = left_merge(fleet_info, db_table, **kwargs)
    COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE = [
        "fleet_id", #
        "region_id", #
        "vehicle_model_id", #
        "purchase_date",
        "licence_plate", #
        "end_of_contract_date", #
        "id",
        #"updated_at",
        #"created_at",
        "activation_status", #
    ]
    fleet_info = fleet_info[COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE]
    for col in COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE:
        if not col in fleet_info.columns:
            print(f"Column {col} not found in fleet_info, adding NANs")
            fleet_info[col] = pd.NA

    # Specify the primary key column in your database table
    key_column = "id"

    # Define the ON CONFLICT clause dynamically to handle column names with spaces
    set_clause = ", ".join([
        f'"{col}" = EXCLUDED."{col}"' if " " in col else f"{col} = EXCLUDED.{col}"
        for col in fleet_info.columns if col != key_column
    ])

    # Convert DataFrame to a list of dictionaries for binding
    data_to_insert = fleet_info.to_dict(orient='records')

    # Perform the upsert operation in a single query
    sql = text(f"""
        INSERT INTO vehicle ({', '.join(fleet_info.columns)})
        VALUES ({', '.join(f":{col}" for col in fleet_info.columns)})
        ON CONFLICT ({key_column}) DO UPDATE 
        SET {set_clause};
    """)

    # Execute the query with all rows in `data_to_insert` as a batch
    connection.execute(sql, parameters=data_to_insert)

    return fleet_info

if __name__ == "__main__":
    #single_dataframe_script_main(update_fleet_info)
    single_dataframe_script_main(update_db_vehicle_table)

fleet_info = get_fleet_info()
