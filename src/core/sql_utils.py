from logging import getLogger

import sqlalchemy as sa
from sqlalchemy import Engine, create_engine, text

from core.config import DB_URI_FORMAT_KEYS, DB_URI_FORMAT_STR
from core.env_utils import get_env_var
from core.logging_utils import set_level_of_loggers_with_prefix
from contextlib import contextmanager

logger = getLogger(__name__)
set_level_of_loggers_with_prefix("INFO", "sql_utils")

def get_sqlalchemy_engine() -> Engine:
    db_uri_format_dict = {key: get_env_var(key) for key in DB_URI_FORMAT_KEYS}
    db_uri = DB_URI_FORMAT_STR.format(**db_uri_format_dict)
    engine = create_engine(db_uri)

    return engine

engine = get_sqlalchemy_engine()
connection = engine.connect()

@contextmanager
def get_connection():
    """Context manager pour obtenir une connexion à la base de données"""
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()

if __name__ == "__main__":
    print(engine)
