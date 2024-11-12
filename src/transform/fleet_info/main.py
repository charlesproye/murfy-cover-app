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

    update_table_with_vin(fleet_info.dropna(subset=[ "vin"], how="any"), "vehicle", "vin")
    
    return fleet_info

def update_table_with_vin(df:DF, table_name:str, key_col:str):
    """
    Update a table using the vin column to align with DataFrame rows.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing new data and `vin` for updating.
        table_name (str): The name of the table in the database to update.
        
    Returns:
        None
    """
    
    # Convert datetime columns in the DataFrame if needed
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = pd.to_datetime(df[col]).dt.to_pydatetime()
    rdb_table = pd.read_sql_table(table_name, connection).dropna(subset=[key_col])
    rdb_table_keys = rdb_table[key_col]
    df = df.set_index(key_col, drop=False).loc[rdb_table_keys]

    # Prepare the column names for the update statement

    #nil_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
    #df["vehicle_model_id"] = df["vehicle_model_id"].fillna(nil_uuid)
    #df["region_id"] = df["region_id"].fillna(nil_uuid)
    #df["fleet_id"] = df["fleet_id"].fillna(nil_uuid)
    
    columns = df.columns.drop("vin").intersection(rdb_table.columns)
    df = df[columns]
    print(columns)
    
    # Open a connection and begin a transaction
    with engine.connect() as conn:
        with conn.begin():  # This starts a transaction
            # Iterate through each row and execute an update based on vin
            for vin, row in df.iterrows():
                row = row.dropna()
                print(row)
                set_clause = ', '.join([f"{col} = :{col}_value" for col in row.index])
                update_statement = text(f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE vin = :vin
                """)
                
                # Prepare the row values for the update statement
                parameters = {f"{col}_value": row[col] for col in row.index}
                parameters['vin'] = vin
                
                # Execute the update statement
                conn.execute(update_statement, parameters)

    print(f"Table '{table_name}' successfully updated using `vin`.")


if __name__ == "__main__":
    #single_dataframe_script_main(update_fleet_info)
    single_dataframe_script_main(update_db_vehicle_table)

fleet_info = get_fleet_info()
