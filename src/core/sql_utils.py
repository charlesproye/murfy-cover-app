from logging import getLogger, Logger
from uuid import uuid4

from sqlalchemy import Engine, create_engine, text, inspect
from sqlalchemy import Connection as Con
from contextlib import contextmanager

from core.pandas_utils import *
from core.config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR, DB_URI_FORMAT_KEYS_PROD, DB_URI_FORMAT_STR_PROD
from core.env_utils import get_env_var

logger = getLogger("core.sql_utils")

def get_sqlalchemy_engine() -> Engine:
    db_uri_format_dict = {key: get_env_var(key) for key in DB_URI_FORMAT_KEYS}
    db_uri = DB_URI_FORMAT_STR.format(**db_uri_format_dict)
    engine = create_engine(db_uri)

    return engine

def get_sqlalchemy_engine_prod() -> Engine:
    db_uri_format_dict = {key: get_env_var(key) for key in DB_URI_FORMAT_KEYS_PROD}
    db_uri = DB_URI_FORMAT_STR_PROD.format(**db_uri_format_dict)
    engine = create_engine(db_uri)

    return engine

engine = get_sqlalchemy_engine()
con = engine.connect()

@contextmanager
def get_connection():
    """Context manager pour obtenir une connexion à la base de données"""
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()

def right_inner_merge(
    lhs: pd.DataFrame,
    rhs_name: str,
    left_on: str|list[str],
    right_on: str|list[str],
    update_cols: list[str],
    logger: logging.Logger = logger,
):
    try:
        # Ensure keys are lists
        left_on = [left_on] if isinstance(left_on, str) else left_on
        right_on = [right_on] if isinstance(right_on, str) else right_on

        # Validate column presence
        for col in left_on + update_cols:
            if col not in lhs.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame.")

        # Upload DataFrame to a temporary table
        TMP_TABLE_NAME = "temp_table"
        logger.info("Uploading DataFrame to temporary table...")
        lhs.to_sql(TMP_TABLE_NAME, con, if_exists="replace", index=False)

        # Construct the SQL update query
        set_clause = ", ".join([f"{col} = temp.{col}" for col in update_cols])
        join_condition = " AND ".join([f"{rhs_name}.{r} = temp.{l}" for l, r in zip(left_on, right_on)])

        update_query = text(f"""
        UPDATE {rhs_name}
        SET {set_clause}
        FROM {TMP_TABLE_NAME} AS temp
        WHERE {join_condition};
        """)
        logger.info(f"Executing update query:\n{update_query}")
        with con.begin() as _:
            con.execute(update_query)

            # Drop the temporary table
            logger.info("Dropping temporary table...")
            con.execute(text(f"DROP TABLE IF EXISTS {TMP_TABLE_NAME};"))

        logger.info("Update operation completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the update operation: {e}")
        raise

def left_merge_rdb_table(
        lhs: DF,
        rhs: str,
        left_on: str|list[str],
        right_on: str|list[str],
        src_dest_cols: list|dict|None=None,
        con: Con=con,
        logger: Logger=logger,
    ) -> DF:
    logger.info(f"Left merging {lhs.shape[0]} rows with {rhs} on {left_on} and {right_on}")
    rhs = pd.read_sql_table(rhs, con)
    return left_merge(lhs, rhs, left_on, right_on, src_dest_cols, logger)

def truncate_rdb_table_and_insert_df(df: DF, table_name: str, src_dest_cols: dict[str, str]|list[str], logger:Logger=logger) -> DF:
    logger.debug(f"Truncating {table_name} and inserting a new DF in it.")
    # Preprocess input
    if isinstance(src_dest_cols, list):
        src_dest_cols = dict(zip(src_dest_cols, src_dest_cols))
    df = (
        df
        .rename(columns=src_dest_cols)
        .loc[:, src_dest_cols.values()]
    )
    # Check for a required not null "id" column in the table
    inspector = inspect(engine)
    table_columns_info = inspector.get_columns(table_name)
    notna_cols = [col['name'] for col in table_columns_info if not col['nullable']]
    # Add "id" column if it is required and not present
    if "id" in notna_cols and not "id" in df.columns:
        logger.debug(f'"id" column is a mandatory not null col in rdb table {table_name} but is not in lhs. Adding id column to lhs.')
        df["id"] = [uuid4() for _ in range(len(df))]
    # Drop rows with missing values in required columns
    df = df.dropna(subset=df.columns.intersection(notna_cols), how="any")
    # Update tbe table
    with engine.begin() as conn:  
        conn.execute(text(f"DELETE FROM {table_name}"))  # Delete all rows
        df.to_sql(table_name, conn, if_exists="append", index=False)
    return df

