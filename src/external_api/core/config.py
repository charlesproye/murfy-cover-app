import os
from typing import Any

import dotenv
from pydantic import EmailStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration, loaded from environment variables and default values.
    """

    dotenv.load_dotenv()

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=True
    )
    # Environment of the app (proxy or local)
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "proxy")

    # API
    API_VERSION: str = "v1"
    API_V1_STR: str = f"/{API_VERSION}"
    PROJECT_NAME: str = "EValue"

    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    ALGORITHM: str = os.getenv("ALGORITHM")
    ENCRYPT_KEY: str = os.getenv("ENCRYPT_KEY")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(
        os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60")
    )  # 1 hour
    REFRESH_TOKEN_EXPIRE_MINUTES: int = int(
        os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", "144000")
    )  # 100 days
    COOKIE_SECURE: bool = os.getenv("COOKIE_SECURE", "true").lower() == "true"
    COOKIE_DOMAIN: str = os.getenv("COOKIE_DOMAIN", "localhost")

    # Server
    BACKEND_CORS_ORIGINS: list[str] = ["*"]
    WEB_CONCURRENCY: int = int(os.getenv("WEB_CONCURRENCY", "4"))
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

    # Database
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "20"))
    POOL_SIZE: int = int(os.getenv("POOL_SIZE", "5"))
    MAX_OVERFLOW: int = int(os.getenv("MAX_OVERFLOW", "10"))
    POOL_TIMEOUT: int = int(os.getenv("POOL_TIMEOUT", "30"))
    POOL_RECYCLE: int = int(os.getenv("POOL_RECYCLE", "1800"))

    # Database data
    DB_DATA_EV_USER: str = os.environ["DB_DATA_EV_USER"]
    DB_DATA_EV_PASSWORD: str = os.environ["DB_DATA_EV_PASSWORD"]
    DB_DATA_EV_HOST: str = os.environ["DB_DATA_EV_HOST"]
    DB_DATA_EV_PORT: int | str = os.environ["DB_DATA_EV_PORT"]
    DB_DATA_EV_NAME: str = os.environ["DB_DATA_EV_NAME"]

    ASYNC_DB_DATA_EV_URI: str | None = None
    DB_DATA_EV_URI: str | None = None

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "")
    REDIS_PORT: str = os.getenv("REDIS_PORT", "")
    REDIS_USER: str = os.getenv("REDIS_USER", "")
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")

    # S3
    S3_BUCKET: str = os.getenv("S3_BUCKET", "")
    S3_KEY: str = os.getenv("S3_KEY", "")
    S3_SECRET: str = os.getenv("S3_SECRET", "")
    S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "")

    # SMTP
    SMTP_EMAIL: str = os.getenv("SMTP_EMAIL", "")
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    SMTP_HOST: str = os.getenv("SMTP_HOST", "")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))

    # Default user
    FIRST_SUPERUSER_EMAIL: EmailStr | None = None
    FIRST_SUPERUSER_PASSWORD: str | None = None

    @field_validator("ASYNC_DB_DATA_EV_URI", mode="before")
    @classmethod
    def assemble_async_db_data_connection(cls, v: str | None, info) -> Any:
        if isinstance(v, str):
            return v
        # Get values safely with fallbacks
        values = info.data
        username = values.get("DB_DATA_EV_USER", "")
        password = values.get("DB_DATA_EV_PASSWORD", "")
        host = values.get("DB_DATA_EV_HOST", "")
        port_str = values.get("DB_DATA_EV_PORT", "")
        db_name = values.get("DB_DATA_EV_NAME", "")

        # Handle potential None values
        port = int(port_str) if port_str else None
        path = f"/{db_name.lstrip('/')}" if db_name else "/rdb"

        # Build URL manually instead of using PostgresDsn.build
        if all([username, password, host, port]):
            return f"postgresql+asyncpg://{username}:{password}@{host}:{port}{path}"
        else:
            # Return a default or None if critical components are missing
            return None

    @field_validator("DB_DATA_EV_URI", mode="before")
    @classmethod
    def assemble_db_data_ev_connection(cls, v: str | None, info: dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v

        user = info.data.get("DB_DATA_EV_USER")
        password = info.data.get("DB_DATA_EV_PASSWORD")
        host = info.data.get("DB_DATA_EV_HOST")
        port = info.data.get("DB_DATA_EV_PORT", "5432")
        db = info.data.get("DB_DATA_EV_NAME", "")

        return f"postgresql://{user}:{password}@{host}:{port}/{db}"

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: str | list[str]) -> list[str] | str:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)


# Singleton for settings
settings = Settings()

