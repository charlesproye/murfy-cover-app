from logging import getLogger
from uuid import uuid4

from sqlalchemy import Engine, create_engine, text, inspect
from sqlalchemy import Connection as Con
from rich.progress import Progress
from contextlib import contextmanager

from core.pandas_utils import *
from core.config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR
from core.env_utils import get_env_var

logger = getLogger("core.sql_utils")

def get_sqlalchemy_engine() -> Engine:
    db_uri_format_dict = {key: get_env_var(key) for key in DB_URI_FORMAT_KEYS}
    db_uri = DB_URI_FORMAT_STR.format(**db_uri_format_dict)
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



def update_row(row: Series, table: str, key_col: str, progress: Progress, task_id: int):
    row = row.dropna()
    set_clause = ', '.join([f"{col} = :{col}" for col in row.index])
    update_statement = text(f"""
        UPDATE {table}
        SET {set_clause}
        WHERE {key_col} = :key_value
    """)
    parameters = row.to_dict() | {"key_value": row[key_col]}
    con.execute(update_statement, parameters)
    progress.update(task_id, advance=1)

def insert_row(row: Series, table: str, key_col: str, progress: Progress, task_id: int):
    row = row.dropna()
    columns = ', '.join(row.index)
    values = ', '.join([f":{col}" for col in row.index])
    
    insert_statement = text(f"""
        INSERT INTO {table} ({columns})
        VALUES ({values})
    """)
    logger.debug(f"Inserting row for {key_col}={row.get(key_col, f'no {key_col}')}")
    parameters = {col: row[col] for col in row.index}
    con.execute(insert_statement, parameters)
    progress.update(task_id, advance=1)

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

def right_union_merge_rdb_table(lhs: DF, table: str, left_on: list[str], right_on: list[str], src_dest_cols: list|dict|None=None, dropna=True):
    logger.info(f"Right union merging {len(lhs)} rows on rdb table '{table}'.")
    # Setup
    # Set left_on and right_on to list if they are not already
    left_on = [left_on] if isinstance(left_on, str) else left_on
    right_on = [right_on] if isinstance(right_on, str) else right_on
    # Assert that all the left_on columns are in lhs
    assert all(col in lhs.columns for col in left_on), f"Not all left_on columns are present in lhs:\nleft_on: {left_on}\nlhs columns: {lhs.columns}"
    # Get rhs table and drop rows with null values in right_on columns to prevent having duplicates down the line
    rhs = pd.read_sql_table(table, con)
    if dropna:
        rhs = rhs.dropna(subset=right_on, how="any")
        lhs = lhs.dropna(subset=left_on, how="any")
    src_cols, dest_cols = src_dest_for_left_merge(lhs, rhs, right_on, left_on, src_dest_cols)
    # Rename df columns to match rdb table columns because we will need them to have the same names when we update or insert
    lhs = lhs.rename(columns=dict(zip(left_on, right_on)) | dict(zip(src_cols, dest_cols)))
    # Convert datetime columns in the DataFrame
    for col in lhs.select_dtypes(include=["datetime64[ns]"]).columns:
        lhs[col] = pd.to_datetime(lhs[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Get metadata of the table to find not-null columns and remove rows with null elements in those columns
    inspector = inspect(engine)
    columns_info = inspector.get_columns(table)
    notna_cols = [col['name'] for col in columns_info if not col['nullable']]
    lhs = lhs.dropna(subset=lhs.columns.intersection(notna_cols), how="any")
    if "id" in notna_cols:
        if "id" not in lhs.columns:
            logger.debug(f'"id" column is a mandatory not null col in rdb table {table} but is not in lhs. Adding id column to lhs.')
            dest_cols.append("id")
            lhs["id"] = [uuid4() for _ in range(len(lhs))]
        else:
            logger.debug(f'filling any null values in id column with uuid4')
            na_id_mask = lhs["id"].isna()
            lhs.loc[na_id_mask, "id"] = [uuid4() for _ in range(na_id_mask.sum())]
        dest_cols.append("id")
    # Split DataFrame into records to update and records to insert
    # Get existing key records from the table to know which ones will be updated
    rdb_table_keys = MultiIdx.from_frame(rhs[right_on])
    df_keys = MultiIdx.from_frame(lhs[right_on]) # Use right_on instead of left_on because the columns are renamed
    existing_keys_mask = df_keys.isin(rdb_table_keys)
    cols = Idx(right_on + dest_cols).drop_duplicates()
    df_to_update = lhs.loc[existing_keys_mask, cols]
    df_to_insert = lhs.loc[~existing_keys_mask, cols]

    logger.debug(f"Inserting {len(df_to_insert)}({len(df_to_insert)/len(lhs)*100:.2f}%) rows.")
    logger.debug(f"Updating {len(df_to_update)}({len(df_to_update )/len(lhs)*100:.2f}%) rows.")

    with Progress(transient=True) as progress:
        update_task = progress.add_task("Updating rows", total=len(df_to_update))
        df_to_update.apply(update_row, axis=1, table=table, key_cols=right_on, progress=progress, task_id=update_task)
        insert_task = progress.add_task("Inserting rows", total=len(df_to_insert))
        df_to_insert.apply(insert_row, axis=1, table=table, key_cols=right_on, progress=progress, task_id=insert_task)

    con.commit()

    return lhs[cols]

def update_row(row: Series, table: str, key_cols: list[str]|str, progress: Progress, task_id: int):
    row = row.dropna()
    set_clause = ', '.join([f"{col} = :{col}" for col in row.index if col != "id" and col not in key_cols])
    if len(set_clause) == 0:
        return
    if isinstance(key_cols, str):
        where_clause = f"{key_cols} = :key_value"
        parameters = row.to_dict() | {"key_value": row[key_cols]}
    else:
        where_clause = ' AND '.join([f"{col} = :{col}" for col in key_cols])
        parameters = row.to_dict() | {col: row[col] for col in key_cols}
    
    update_statement = text(f"""
        UPDATE {table}
        SET {set_clause}
        WHERE {where_clause}
    """)
    
    con.execute(update_statement, parameters)
    progress.update(task_id, advance=1)

def insert_row(row: Series, table: str, key_cols: list[str]|str, progress: Progress, task_id: int):
    row = row.dropna()
    columns = ', '.join(row.index)
    values = ', '.join([f":{col}" for col in row.index])
    if len(columns) == 0 or len(values) == 0:
        return
    
    insert_statement = text(f"""
        INSERT INTO {table} ({columns})
        VALUES ({values})
    """)
    parameters = {col: row[col] for col in row.index}
    con.execute(insert_statement, parameters)
    progress.update(task_id, advance=1)

