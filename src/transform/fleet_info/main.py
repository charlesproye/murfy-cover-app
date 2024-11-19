from logging import getLogger

from sqlalchemy import text
import uuid
from datetime import datetime as DT
from rich import print

from core.pandas_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from rich.progress import track
from core.sql_utils import connection
from core.ev_models_info import models_info
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.ayvens_fleet_info import fleet_info as ayvens_fleet_info
from transform.fleet_info.tesla_fleet_info import test_tesla_fleet_info
from transform.fleet_info.tesla_fleet_info import followed_tesla_vehicles_info
from transform.fleet_info.config import RDB_TABLES_MERGE_KWARGS

logger = getLogger("transform.fleet_info")

def get_fleet_info() -> DF:
    return (
        pd.concat((ayvens_fleet_info, test_tesla_fleet_info))
        .eval("model = model.str.lower()")
        .eval("version = version.str.lower()")
        .pipe(set_all_str_cols_to_lower, but=["vin"])
        .pipe(left_merge, followed_tesla_vehicles_info, left_on=["vin"], right_on=["vin"], src_dest_cols=["model", "version"])
        .pipe(left_merge, models_info, left_on=["model", "version"], right_on=["model", "version"])
    )

def update_db_vehicle_table() -> DF:
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
    
    # Set purchase date to now for new entries
    fleet_info["purchase_date"] = DT.now()

    fleet_info = fleet_info.dropna(subset=["vin"])
    
    # Generate new UUIDs for new entries
    fleet_info["id"] = Series([str(uuid.uuid4()) for _ in range(len(fleet_info))], dtype="string")
    
    # Drop rows with missing essential data
    clean_fleet_info = fleet_info.dropna(subset=["vin"])
    
    # Update the vehicle table
    update_table_with_df(clean_fleet_info, "vehicle", "vin")
    
    return fleet_info

def update_table_with_df(df: DF, table_name: str, key_col: str):
    """
    Update a table using the key_col column to align with DataFrame rows.
    Performs UPDATE for existing records and INSERT for new ones.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing new data and `key_col` for updating.
        table_name (str): The name of the table in the database to update.
        key_col (str): The column to use as the key for matching records.
    """
    # Convert datetime columns in the DataFrame
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Get existing records from the table
    rdb_table = pd.read_sql_table(table_name, connection).dropna(subset=[key_col])
    existing_keys = set(rdb_table[key_col])
    
    # Split DataFrame into records to update and records to insert
    df_to_update = df[df[key_col].isin(existing_keys)]
    df_to_insert = df[~df[key_col].isin(existing_keys)]
    
    # Handle Updates
    if not df_to_update.empty:
        update_columns = df_to_update.columns.drop([key_col, 'id']).intersection(rdb_table.columns)
        for key, row in track(df_to_update.iterrows(), description="Updating existing rows", total=len(df_to_update)):
            row = row[update_columns].dropna()
            if row.empty:
                continue
                
            set_clause = ', '.join([f"{col} = :{col}" for col in row.index])
            update_statement = text(f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE {key_col} = :key_value
            """)

            # Prepare parameters including the key
            parameters = {col: row[col] for col in row.index}
            parameters['key_value'] = df_to_update.loc[key, key_col]

            connection.execute(update_statement, parameters)
    connection.commit()
    
    # Handle Inserts
    if not df_to_insert.empty:
        insert_columns = df_to_insert.columns.intersection(rdb_table.columns)
        df_to_insert = df_to_insert[insert_columns]
        
        for _, row in track(df_to_insert.iterrows(), description="Inserting new rows"):
            if row.isna()["id"]:
                logger.warning(f"row is empty for vin={row.get('vin', 'no vin')}")
                continue
            row = row.dropna()
            columns = ', '.join(row.index)
            values = ', '.join([f":{col}" for col in row.index])
            
            insert_statement = text(f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({values})
            """)
            
            parameters = {col: row[col] for col in row.index}
            connection.execute(insert_statement, parameters)
    
    # Commit all changes
    connection.commit()

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.fleet_info")
    single_dataframe_script_main(update_db_vehicle_table)

fleet_info = get_fleet_info()
