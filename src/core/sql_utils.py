from logging import getLogger

from sqlalchemy import Engine, create_engine, text

from rich.progress import track
from core.pandas_utils import *
from core.config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR
from core.env_utils import get_env_var
from core.logging_utils import set_level_of_loggers_with_prefix

logger = getLogger(__name__)
set_level_of_loggers_with_prefix("INFO", "sql_utils")

def get_sqlalchemy_engine() -> Engine:
    db_uri_format_dict = {key: get_env_var(key) for key in DB_URI_FORMAT_KEYS}
    db_uri = DB_URI_FORMAT_STR.format(**db_uri_format_dict)
    engine = create_engine(db_uri)

    return engine

def update_table_with_df(df: DF, table_name: str, key_col: str):
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


engine = get_sqlalchemy_engine()
connection = engine.connect()

if __name__ == "__main__":
    print(engine)
