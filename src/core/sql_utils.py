from logging import getLogger, Logger
from uuid import uuid4

from sqlalchemy import Engine, create_engine, text, inspect
from sqlalchemy import Connection as Con
from contextlib import contextmanager

from .spark_utils import *
from .pandas_utils import *
from .config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR, DB_URI_FORMAT_KEYS_PROD, DB_URI_FORMAT_STR_PROD
from .env_utils import get_env_var
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid

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


def left_merge_rdb_table_spark(
        lhs,
        rhs: str,
        left_on: str|list[str],
        right_on: str|list[str],
        src_dest_cols: list|dict|None=None,
        logger: Logger=logger,
    ):
    """
    Version Spark complète avec src_dest_cols
    """
    logger.info(f"Left merging Spark DataFrame with {rhs} on {left_on} and {right_on}")
    
    # Lire la table RDB en pandas
    rhs_pandas = pd.read_sql_table(rhs, engine)
    
    # Convertir tous les types problématiques en string
    for col in rhs_pandas.columns:
        if rhs_pandas[col].dtype == 'object':
            rhs_pandas[col] = rhs_pandas[col].astype(str)
        elif 'datetime' in str(rhs_pandas[col].dtype):
            rhs_pandas[col] = rhs_pandas[col].astype(str)
    
    # Convertir en DataFrame Spark
    rhs_spark = lhs.sparkSession.createDataFrame(rhs_pandas)
    
    # Gérer src_dest_cols
    if src_dest_cols:
        if isinstance(src_dest_cols, list):
            src_dest_cols = dict(zip(src_dest_cols, src_dest_cols))
        
        # Renommer les colonnes de rhs selon src_dest_cols
        for old_name, new_name in src_dest_cols.items():
            if old_name in rhs_spark.columns:
                rhs_spark = rhs_spark.withColumnRenamed(old_name, new_name)
    
    # Effectuer le left join
    if isinstance(left_on, str):
        left_on = [left_on]
    if isinstance(right_on, str):
        right_on = [right_on]
    
    # Créer la condition de join
    join_condition = " AND ".join([f"lhs.{left_col} = rhs.{right_col}" 
                                  for left_col, right_col in zip(left_on, right_on)])
    
    # Effectuer le left join
    result = lhs.alias("lhs").join(
        rhs_spark.alias("rhs"),
        F.expr(join_condition),
        "left"
    )
    
    # Sélectionner les colonnes selon src_dest_cols
    if src_dest_cols:
        select_cols = []
        # Ajouter toutes les colonnes de lhs
        for col_name in lhs.columns:
            select_cols.append(f"lhs.{col_name}")
        
        # Ajouter les colonnes de rhs (sauf celles déjà présentes)
        for col_name in rhs_spark.columns:
            if col_name not in left_on:
                select_cols.append(f"rhs.{col_name}")
        
        result = result.select(*select_cols)
    
    return result

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


def truncate_rdb_table_and_insert_df_spark(
    df, 
    table_name: str, 
    src_dest_cols: dict[str, str]|list[str], 
    logger: Logger=logger,
    is_prod: bool = False,
):
    """
    Version Spark de truncate_rdb_table_and_insert_df
    """
    logger.debug(f"Truncating {table_name} and inserting a new Spark DataFrame in it.")
    
    # Preprocess input
    if isinstance(src_dest_cols, list):
        src_dest_cols = dict(zip(src_dest_cols, src_dest_cols))
    
    # Renommer les colonnes
    for old_name, new_name in src_dest_cols.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    # Sélectionner seulement les colonnes de destination
    df = df.select(list(src_dest_cols.values()))
    
    # Check for a required not null "id" column in the table
    inspector = inspect(get_sqlalchemy_engine(is_prod))
    table_columns_info = inspector.get_columns(table_name)
    notna_cols = [col['name'] for col in table_columns_info if not col['nullable']]
    
    # Add "id" column if it is required and not present
    if "id" in notna_cols and "id" not in df.columns:
        logger.debug(f'"id" column is a mandatory not null col in rdb table {table_name} but is not in lhs. Adding id column to lhs.')
        # Utiliser uuid4 au lieu de monotonically_increasing_id pour UUID
        
        @udf(StringType())
        def generate_uuid():
            return str(uuid.uuid4())
        
        df = df.withColumn("id", generate_uuid())
    
    # Drop rows with missing values in required columns
    for col_name in notna_cols:
        if col_name in df.columns:
            df = df.filter(col(col_name).isNotNull())
    
    # Convertir en pandas pour l'insertion
    df_pandas = df.toPandas()
    
    # Update the table
    with get_sqlalchemy_engine(is_prod).begin() as conn:  
        conn.execute(text(f"DELETE FROM {table_name}"))  # Delete all rows
        df_pandas.to_sql(table_name, conn, if_exists="append", index=False)
    
    return df
