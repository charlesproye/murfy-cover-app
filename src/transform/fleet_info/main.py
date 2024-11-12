from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
import uuid
import csv
from io import StringIO
from sqlalchemy import create_engine, Table, MetaData, select, update, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from datetime import datetime as DT
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy.dialects.postgresql import UUID
from rich import print
#import numpy as np
#register_adapter(np.int64, AsIs)

from core.pandas_utils import *
from core.sql_utils import connection, engine
from core.ev_models_info import models_info
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.config import COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE
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
    
    # Convert date columns to the correct format
    if 'end_of_contract_date' in fleet_info.columns:
        fleet_info['end_of_contract_date'] = pd.to_datetime(
            fleet_info['end_of_contract_date'], 
            format='%d-%m-%Y', 
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')
    
    for table_name, kwargs in RDB_TABLES_MERGE_KWARGS.items():
        db_table = pd.read_sql_table(table_name, connection)
        fleet_info = left_merge(fleet_info, db_table, **kwargs)
    fleet_info["purchase_date"] = DT.now()
    
    fleet_info["id"] = Series([str(uuid.uuid4()) for _ in range(len(fleet_info))], dtype="string")
    
    #for col in COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE:
        #if not col in fleet_info.columns:
            #fleet_info[col] = pd.NA

    #fleet_info = fleet_info[COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE]

    #with pd.option_context('display.max_rows', None):
        #print(fleet_info.isna().all())
        #print(fleet_info.dropna(subset=["fleet_id", "region_id", "vehicle_model_id"], how="any"))

    update_table_with_df(fleet_info.dropna(subset=[ "vin"], how="any"), "vehicle", "vin")
    
    return fleet_info

def update_table_with_df(df:DF, table_name:str, key_col:str):
    """
    Update a table using the key_col column to align with DataFrame rows.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing new data and `key_col` for updating.
        table_name (str): The name of the table in the database to update.
        
    Returns:
        None
    """
    
    # Convert datetime columns in the DataFrame if needed
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = pd.to_datetime(df[col]).dt.to_pydatetime()
    rdb_table = pd.read_sql_table(table_name, connection).dropna(subset=[key_col])
    rdb_table_keys = rdb_table[key_col]
    update_src_df = df.set_index(key_col, drop=False).loc[rdb_table_keys]

    columns = update_src_df.columns.drop(key_col).intersection(rdb_table.columns)
    update_src_df = update_src_df[columns]
    
    # Iterate through each row and execute an update based on key_col
    for key, row in update_src_df.iterrows():
        row = row.dropna()
        set_clause = ', '.join([f"{col} = :{col}_value" for col in row.index])
        update_statement = text(f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE {key_col} = :key_col
        """)

        # Prepare the row values for the update statement
        parameters = {f"{col}_value": row[col] for col in row.index}
        parameters['key_col'] = key
        
        print(update_statement)
        print(parameters)

        # Execute the update statement
        connection.execute(update_statement, parameters)

    # insert_src_df = df[~df[key_col].isin(rdb_table_keys)][df.columns.intersection(rdb_table.columns)]
    # for key, row in insert_src_df.iterrows():
    #     row = row.dropna()
    #     columns = ', '.join(row.index)
    #     values = ', '.join([f":{col}_value" for col in row.index])
    #     insert_statement = text(f"""
    #         INSERT INTO {table_name} ({columns})
    #         VALUES ({values})
    #     """)
        
    #     # Prepare the row values for the insert statement
    #     parameters = {f"{col}_value": row[col] for col in row.index}
        
    #     print(insert_statement)
    #     print(parameters)

    #     # Execute the insert statement
    #     connection.execute(insert_statement, parameters)

if __name__ == "__main__":
    #single_dataframe_script_main(update_fleet_info)
    single_dataframe_script_main(update_db_vehicle_table)

fleet_info = get_fleet_info()
