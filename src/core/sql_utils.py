from logging import getLogger

from sqlalchemy import Engine, create_engine, text, inspect

from core.pandas_utils import *
from core.config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR
from core.env_utils import get_env_var
from core.logging_utils import set_level_of_loggers_with_prefix

logger = getLogger(__name__)

def get_sqlalchemy_engine() -> Engine:
    db_uri_format_dict = {key: get_env_var(key) for key in DB_URI_FORMAT_KEYS}
    db_uri = DB_URI_FORMAT_STR.format(**db_uri_format_dict)
    engine = create_engine(db_uri)

    return engine

def upsert_table_with_df(df: DF, table: str, key_col: str, logger: Logger=logger):
    logger.info(f"Upserting table {table} with {len(df)} rows")
    # Convert datetime columns in the DataFrame
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Get existing records from the table
    rdb_table = pd.read_sql_table(table, connection).dropna(subset=[key_col])
    existing_keys_mask = df[key_col].isin(rdb_table[key_col])
    insert_columns = df.columns.intersection(rdb_table.columns)
    # Get metadata of the table to find not-null columns and remove rows with null columns
    inspector = inspect(engine)
    columns_info = inspector.get_columns(table)
    notna_cols = [col['name'] for col in columns_info if not col['nullable']]
    df = df.dropna(subset=notna_cols, how="any")
    # Split DataFrame into records to update and records to insert
    df_to_update = df.loc[existing_keys_mask, insert_columns]
    df_to_insert = df.loc[~existing_keys_mask, insert_columns]

    df_to_update.apply(update_row, axis=1, table=table, key_col=key_col)
    df_to_insert.apply(insert_row, axis=1, table=table, key_col=key_col)

    connection.commit()

def update_row(row: Series, table: str, key_col: str):
    row = row.dropna()
    set_clause = ', '.join([f"{col} = :{col}" for col in row.index])
    update_statement = text(f"""
        UPDATE {table}
        SET {set_clause}
        WHERE {key_col} = :key_value
    """)
    parameters = row.to_dict() | {"key_value": row[key_col]}
    connection.execute(update_statement, parameters)

def insert_row(row: Series, table: str, key_col: str):
    row = row.dropna()
    columns = ', '.join(row.index)
    values = ', '.join([f":{col}" for col in row.index])
    
    insert_statement = text(f"""
        INSERT INTO {table} ({columns})
        VALUES ({values})
    """)
    logger.debug(f"Inserting row for {key_col}={row.get(key_col, f'no {key_col}')}")
    parameters = {col: row[col] for col in row.index}
    connection.execute(insert_statement, parameters)

engine = get_sqlalchemy_engine()
connection = engine.connect()

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("INFO", "sql_utils")
    print(engine)

