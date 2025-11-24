import logging
import os

import dotenv
from pydantic import EmailStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOGGER = logging.getLogger(__name__)


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

    # Tesla
    TESLA_CLIENT_ID: str = os.getenv("TESLA_CLIENT_ID", "")
    TESLA_CLIENT_SECRET: str = os.getenv("TESLA_CLIENT_SECRET", "")
    if not all([TESLA_CLIENT_ID, TESLA_CLIENT_SECRET]):
        LOGGER.warning(
            "TESLA_CLIENT_ID or TESLA_CLIENT_SECRET is not set. Will not be able to use Tesla API."
        )

    # Default user
    FIRST_SUPERUSER_EMAIL: EmailStr | None = None
    FIRST_SUPERUSER_PASSWORD: str | None = None

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
