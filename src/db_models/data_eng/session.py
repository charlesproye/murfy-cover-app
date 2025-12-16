"""Engine and session helpers for the data-engineering database."""

from functools import lru_cache

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from db_models.core.config import db_settings


@lru_cache(maxsize=1)
def get_data_eng_engine() -> Engine:
    """Return a cached SQLAlchemy engine for the data-engineering DB."""
    db_uri = db_settings.DB_DATA_ENG_URI
    if not db_uri:
        raise ValueError(
            "DB_DATA_ENG_URI is not configured. "
            "Set DB_DATA_ENG_* environment variables to manage the data-engineering DB."
        )
    return create_engine(db_uri, future=True)


def get_data_eng_sessionmaker(autocommit: bool = False, autoflush: bool = False):
    """Sessionmaker factory bound to the data-engineering engine."""
    engine = get_data_eng_engine()
    return sessionmaker(
        bind=engine,
        autocommit=autocommit,
        autoflush=autoflush,
        expire_on_commit=False,
        future=True,
    )


def get_data_eng_session(autocommit: bool = False, autoflush: bool = False):
    """Convenience helper to get a single session instance."""
    return get_data_eng_sessionmaker(autocommit=autocommit, autoflush=autoflush)()


__all__ = [
    "get_data_eng_engine",
    "get_data_eng_session",
    "get_data_eng_sessionmaker",
]
