from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from contextlib import contextmanager
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from logging import getLogger

import pandas as pd
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import sessionmaker

from core.pandas_utils import DF, left_merge
from db_models.core.config import db_settings

LOGGER = getLogger(__name__)


def get_sqlalchemy_engine(is_prod: bool = False, db_name: str = "rdb") -> Engine:
    """Get a SQLAlchemy engine for the specified database.

    All connections use SSL encryption in transit for remote databases.
    Localhost connections skip SSL for development convenience.

    Args:
        is_prod (bool): Deprecated. No longer used. Configure via environment variables instead.
        db_name (str): Database name - "rdb" (default) or "data-engineering"

    Returns:
        Engine: SQLAlchemy engine instance with SSL enabled for remote connections
    """
    if is_prod:
        LOGGER.warning(
            "is_prod parameter is deprecated and ignored. "
            "Configure production database via DB_DATA_EV_* environment variables instead."
        )

    if db_name == "rdb":
        db_uri = db_settings.DB_DATA_EV_URI
    elif db_name == "data-engineering":
        db_uri = db_settings.DB_DATA_ENG_URI
    else:
        raise ValueError(f"Unknown database name: {db_name}")

    if not db_uri:
        raise ValueError(
            f"Database URI not configured for db_name={db_name}. "
            f"Check DB_DATA_EV_* or DB_DATA_ENG_* environment variables."
        )

    return create_engine(db_uri)


def get_session(is_prod: bool = False, db_name: str = "rdb"):
    engine = get_sqlalchemy_engine(is_prod, db_name)
    Session = sessionmaker(bind=engine)
    return Session()


@contextmanager
def get_connection(is_prod: bool = False, db_name: str = "rdb"):
    """Context manager pour obtenir une connexion à la base de données

    Args:
        is_prod (bool): Deprecated. No longer used.
        db_name (str): Database name - "rdb" (default) or "data-engineering"
    """
    engine = get_sqlalchemy_engine(is_prod, db_name)
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()


def left_merge_rdb_table(
    lhs: DF,
    rhs: str,
    left_on: str | list[str],
    right_on: str | list[str],
    src_dest_cols: list | dict | None = None,
    is_prod: bool = False,
) -> DF:
    """
    Reads the rdb table with the name `rhs` and performs a left_merge on the df with it.
    Take a look at the doc string of `core.pandas_utils.left_merge` to understand how the other arguments are used.
    """
    LOGGER.info(
        f"Left merging {lhs.shape[0]} rows with {rhs} on {left_on} and {right_on}"
    )
    engine = get_sqlalchemy_engine(is_prod)
    rhs = pd.read_sql_table(rhs, engine)
    return left_merge(lhs, rhs, left_on, right_on, src_dest_cols)


def insert_df_and_deduplicate(
    df: pd.DataFrame,
    table_name: str,
    key_cols: list[str],
    src_dest_cols: dict[str, str] | list[str],
    uuid_cols: list[str] | None = None,
    batch_size: int = 5000,
    is_prod: bool = False,
    keep_col: str = "created_at",  # colonne pour garder la dernière ligne
):
    """
    Insert a Pandas DataFrame into a Postgres table and remove duplicates.

    - Inserts all rows in batches
    - Deduplicates table on key_cols keeping the last row by keep_col
    - Handles UUID columns and mandatory 'id'
    - Clips numeric columns to avoid NumericValueOutOfRange
    """

    LOGGER.debug(
        f"Inserting into {table_name} in batches of {batch_size} (deduplicating after)."
    )

    # Normalize column mapping
    if isinstance(src_dest_cols, list):
        src_dest_cols = dict(zip(src_dest_cols, src_dest_cols, strict=False))
    df = df.rename(columns=src_dest_cols)

    df = df.dropna(subset=["vehicle_id"], how="any")

    # Keep only the columns that exist in destination
    df = df[list(src_dest_cols.values())]

    LOGGER.debug(f'"id" column is mandatory in {table_name} but missing. Adding UUIDs.')
    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

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
        engine,
    )
    numeric_map = {
        row["column_name"]: (row["numeric_precision"], row["numeric_scale"])
        for _, row in numeric_info.iterrows()
        if row["column_name"] in df.columns
    }

    for col, (prec, scale) in numeric_map.items():
        if prec and scale:
            max_value = 10 ** (prec - scale) - 10 ** (-scale)
            min_value = -max_value

            def safe_decimal(x, scale=scale):
                if pd.isna(x):
                    return None
                try:
                    return float(
                        Decimal(x).quantize(
                            Decimal(f"1.{'0' * scale}"), rounding=ROUND_HALF_UP
                        )
                    )
                except (InvalidOperation, ValueError, TypeError):
                    return None  # ou float('nan') si tu préfères

            df[col] = df[col].apply(safe_decimal)
            df[col] = df[col].clip(lower=min_value, upper=max_value)

    # Insert in batches
    for start in range(0, len(df), batch_size):
        df_batch = df.iloc[start : start + batch_size]
        LOGGER.debug(f"Inserting batch {start}-{start + len(df_batch)}...")
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
    LOGGER.debug(f"Removing duplicates in {table_name} keeping last by {keep_col}...")
    with engine.begin() as conn:
        conn.execute(text(dedup_sql))

    return df


# Async engine
_engine: AsyncEngine | None = None
_session_maker = None


def get_async_engine() -> AsyncEngine:
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            db_settings.ASYNC_DB_DATA_EV_URI,
            echo=False,
            future=True,
            pool_size=db_settings.POOL_SIZE,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={"server_settings": {"search_path": "public"}},
        )
    return _engine


def get_async_session_maker():
    """Get or create the session maker."""
    global _session_maker
    if _session_maker is None:
        engine = get_async_engine()
        _session_maker = sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
    return _session_maker


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    session = get_async_session_maker()()
    try:
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
