from logging import getLogger, Logger
from uuid import uuid4

from sqlalchemy import Engine, create_engine, text, inspect
from sqlalchemy import Connection as Con
from contextlib import contextmanager

from .pandas_utils import *
from .config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR, DB_URI_FORMAT_KEYS_PROD, DB_URI_FORMAT_STR_PROD
from .env_utils import get_env_var
import uuid
from decimal import Decimal, ROUND_HALF_UP


logger = getLogger("core.sql_utils")

def get_sqlalchemy_engine(is_prod: bool = False) -> Engine:
    """Get a SQLAlchemy engine for the specified environment.
    
    Args:
        is_prod (bool): If True, uses production database configuration. Defaults to False.
    
    Returns:
        Engine: SQLAlchemy engine instance
    """
    keys = DB_URI_FORMAT_KEYS_PROD if is_prod else DB_URI_FORMAT_KEYS
    format_str = DB_URI_FORMAT_STR_PROD if is_prod else DB_URI_FORMAT_STR
    db_uri_format_dict = {key: get_env_var(key) for key in keys}
    db_uri = format_str.format(**db_uri_format_dict)
    return create_engine(db_uri)

@contextmanager
def get_connection(is_prod: bool = False):
    """Context manager pour obtenir une connexion à la base de données
    
    Args:
        is_prod (bool): If True, uses the production database connection. Defaults to False.
    """
    engine = get_sqlalchemy_engine(is_prod)
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()

def left_merge_rdb_table(
        lhs: DF,
        rhs: str,
        left_on: str|list[str],
        right_on: str|list[str],
        src_dest_cols: list|dict|None=None,
        is_prod: bool = False,
        logger: Logger=logger,
    ) -> DF:
    """
    Reads the rdb table with the name `rhs` and performs a left_merge on the df with it.
    Take a look at the doc string of `core.pandas_utils.left_merge` to understand how the other arguments are used.
    """
    logger.info(f"Left merging {lhs.shape[0]} rows with {rhs} on {left_on} and {right_on}")
    engine = get_sqlalchemy_engine(is_prod)
    rhs = pd.read_sql_table(rhs, engine)
    return left_merge(lhs, rhs, left_on, right_on, src_dest_cols, logger)

def truncate_rdb_table_and_insert_df(
        df: DF, 
        table_name: str, 
        src_dest_cols: dict[str, str]|list[str], 
        is_prod: bool = False,
        logger: Logger=logger
    ) -> DF:
    """
    Warp around `DataFram.to_sql`.    
    Instead of appending on the table or deleting it and creating a new one with the same name, we simply empty the table and fill it with the DF.
    """
    logger.debug(f"Truncating {table_name} and inserting a new DF in it.")
    # Preprocess input
    if isinstance(src_dest_cols, list):
        src_dest_cols = dict(zip(src_dest_cols, src_dest_cols))
    df = (
        df
        .rename(columns=src_dest_cols)
        .loc[:, src_dest_cols.values()]
    )
    
    engine = get_sqlalchemy_engine(is_prod)
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

def insert_df_and_deduplicate(
    df: pd.DataFrame,
    table_name: str,
    key_cols: list[str],
    src_dest_cols: dict[str, str] | list[str],
    logger: Logger,
    uuid_cols: list[str] = [],
    batch_size: int = 5000,
    is_prod: bool = False,
    keep_col: str = "created_at"  # colonne pour garder la dernière ligne
):
    """
    Insert a Pandas DataFrame into a Postgres table and remove duplicates.

    - Inserts all rows in batches
    - Deduplicates table on key_cols keeping the last row by keep_col
    - Handles UUID columns and mandatory 'id'
    - Clips numeric columns to avoid NumericValueOutOfRange
    """

    logger.debug(f"Inserting into {table_name} in batches of {batch_size} (deduplicating after).")

    # Normalize column mapping
    if isinstance(src_dest_cols, list):
        src_dest_cols = dict(zip(src_dest_cols, src_dest_cols))
    df = df.rename(columns=src_dest_cols)

    df = df.dropna(subset=['vehicle_id'], how="any")

    # Keep only the columns that exist in destination
    df = df[list(src_dest_cols.values())]

    logger.debug(f'"id" column is mandatory in {table_name} but missing. Adding UUIDs.')
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]


    # Cast UUID columns to string
    for col in uuid_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)

    engine = get_sqlalchemy_engine(is_prod)

    # Optional: Clip numeric columns based on Postgres precision/scale
    numeric_info = pd.read_sql(
        f"""
        SELECT column_name, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND data_type = 'numeric';
        """,
        engine
    )
    numeric_map = {
        row['column_name']: (row['numeric_precision'], row['numeric_scale'])
        for _, row in numeric_info.iterrows()
        if row['column_name'] in df.columns
    }
    for col, (prec, scale) in numeric_map.items():
        if prec and scale:
            max_value = 10 ** (prec - scale) - 10 ** (-scale)
            min_value = -max_value
            df[col] = df[col].apply(lambda x: None if pd.isna(x)
                                     else float(Decimal(x).quantize(Decimal(f"1.{'0'*scale}"), rounding=ROUND_HALF_UP)))
            df[col] = df[col].clip(lower=min_value, upper=max_value)

    # Insert in batches
    for start in range(0, len(df), batch_size):
        df_batch = df.iloc[start:start + batch_size]
        logger.debug(f"Inserting batch {start}-{start + len(df_batch)}...")
        with engine.begin() as conn:
            df_batch.to_sql(table_name, conn, if_exists="append", index=False)

    # Deduplicate table
    key_cols_sql = ", ".join(key_cols)
    dedup_sql = f"""
    WITH duplicates AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY {key_cols_sql}
                ORDER BY {keep_col} DESC
            ) AS rn
        FROM {table_name}
    )
    DELETE FROM {table_name}
    WHERE ctid IN (
        SELECT ctid
        FROM duplicates
        WHERE rn > 1
    );
    """
    logger.debug(f"Removing duplicates in {table_name} keeping last by {keep_col}...")
    with engine.begin() as conn:
        conn.execute(text(dedup_sql))

    return df
