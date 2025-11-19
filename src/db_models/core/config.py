import logging
import os

from dotenv import load_dotenv
from pydantic import field_validator
from pydantic_settings import BaseSettings

LOGGER = logging.getLogger(__name__)
load_dotenv()


def _build_ssl_params_psycopg2(host: str) -> str:
    if host in ("localhost", "127.0.0.1"):
        LOGGER.warning("SSL is disabled for localhost")
        return ""

    return "?sslmode=verify-full"


def _build_ssl_params_asyncpg(host: str) -> str:
    # https://magicstack.github.io/asyncpg/current/api/index.html#connection
    if host in ("localhost", "127.0.0.1"):
        LOGGER.warning("SSL is disabled for localhost")
        return ""
    return "?ssl=verify-full"


class Settings(BaseSettings):
    """
    Centralized database configuration for the EValue project.

    This is the single source of truth for all database connection strings.
    All database connections include SSL encryption in transit for remote databases.

    Note: Change DB_DATA_EV_* and DB_DATA_ENG_* environment variables per environment
    """

    WEB_CONCURRENCY: int = int(os.getenv("WEB_CONCURRENCY", "4"))
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "100"))
    POOL_SIZE: int = max(DB_POOL_SIZE // WEB_CONCURRENCY, 5)

    # Main database (set different values per environment via env vars)
    DB_DATA_EV_USER: str = "evalue"
    DB_DATA_EV_PASSWORD: str = "evalue"
    DB_DATA_EV_HOST: str = "localhost"
    DB_DATA_EV_PORT: int = 5432
    DB_DATA_EV_NAME: str = "evalue"

    # Data Engineering database (optional, set via env vars if needed)
    DB_DATA_ENG_USER: str = ""
    DB_DATA_ENG_PASSWORD: str = ""
    DB_DATA_ENG_HOST: str = ""
    DB_DATA_ENG_PORT: int = 5432
    DB_DATA_ENG_NAME: str = ""

    # Connection URIs (built automatically with SSL for remote databases)
    ASYNC_DB_DATA_EV_URI: str | None = None
    DB_DATA_EV_URI: str | None = None
    DB_DATA_ENG_URI: str | None = None

    @field_validator("ASYNC_DB_DATA_EV_URI", mode="before")
    @classmethod
    def assemble_async_db_data_ev_uri(cls, v: str | None, info) -> str:
        """Build async dev database URI with SSL for remote connections."""
        if isinstance(v, str):
            return v
        values = info.data
        user = values.get("DB_DATA_EV_USER")
        password = values.get("DB_DATA_EV_PASSWORD")
        host = values.get("DB_DATA_EV_HOST")
        port = values.get("DB_DATA_EV_PORT")
        db_name = values.get("DB_DATA_EV_NAME")
        ssl_params = _build_ssl_params_asyncpg(host)
        return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}{ssl_params}"

    @field_validator("DB_DATA_EV_URI", mode="before")
    @classmethod
    def assemble_sync_db_data_ev_uri(cls, v: str | None, info) -> str:
        """Build sync dev database URI with SSL for remote connections."""
        if isinstance(v, str):
            return v
        values = info.data
        user = values.get("DB_DATA_EV_USER")
        password = values.get("DB_DATA_EV_PASSWORD")
        host = values.get("DB_DATA_EV_HOST")
        port = values.get("DB_DATA_EV_PORT")
        db_name = values.get("DB_DATA_EV_NAME")
        ssl_params = _build_ssl_params_psycopg2(host)
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}{ssl_params}"

    @field_validator("DB_DATA_ENG_URI", mode="before")
    @classmethod
    def assemble_data_eng_db_uri(cls, v: str | None, info) -> str:
        """Build data engineering database URI with SSL for remote connections."""
        if isinstance(v, str):
            return v
        values = info.data
        user = values.get("DB_DATA_ENG_USER")
        password = values.get("DB_DATA_ENG_PASSWORD")
        host = values.get("DB_DATA_ENG_HOST")
        port = values.get("DB_DATA_ENG_PORT")
        db_name = values.get("DB_DATA_ENG_NAME")
        if not all([user, password, host, db_name]):
            return ""
        ssl_params = _build_ssl_params_psycopg2(host)
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}{ssl_params}"


db_settings = Settings()
