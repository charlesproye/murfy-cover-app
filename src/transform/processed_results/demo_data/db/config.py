from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, EmailStr, PostgresDsn, field_validator, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    API_VERSION: str = "v1"
    API_V1_STR: str = f"/api/{API_VERSION}"
    PROJECT_NAME: str = "bib-monitor"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60  # 1 hour
    REFRESH_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 100  # 100 days
    WEB_CONCURRENCY:int  = 4
    DB_POOL_SIZE:int = 20
    POOL_SIZE:int = 5
    MAX_OVERFLOW:int = 10

    DB_AUTH_USER: str = "auth"
    DB_AUTH_PASSWORD: str = "auth"
    DB_AUTH_HOST: str = "localhost"
    DB_AUTH_PORT: Union[int, str] = 5432
    DB_AUTH_NAME: str = "auth"
    ASYNC_DB_AUTH_URI: Optional[str] = None

    DB_DATA_EV_USER: str = "data"
    DB_DATA_EV_PASSWORD: str = "data"
    DB_DATA_EV_HOST: str = "localhost"
    DB_DATA_EV_PORT: Union[int, str] = 5432
    DB_DATA_EV_NAME: str = "data"
    ASYNC_DB_DATA_EV_URI: Optional[str] = None

    REDIS_HOST: str = ""
    REDIS_PORT: str = ""
    REDIS_USER: str = ""
    REDIS_PASSWORD: str = ""

    # @validator("ASYNC_DB_DATA_EV_URI", pre=True)
    # def assemble_async_db_data_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
    #     if isinstance(v, str):
    #         return v
    #     return PostgresDsn.build(
    #         scheme="postgresql+asyncpg",
    #         username=values.get("DB_DATA_EV_USER"),
    #         password=values.get("DB_DATA_EV_PASSWORD"),
    #         host=values.get("DB_DATA_EV_HOST"),
    #         port=int(values.get("DB_DATA_EV_PORT")),
    #         path=f"/{values.get('DB_DATA_EV_NAME').lstrip('/')}",
    #     )

    @field_validator("ASYNC_DB_DATA_EV_URI", mode='before')
    def assemble_async_db_data_connection(cls, v: Optional[str], info) -> Any:
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

    FIRST_SUPERUSER_EMAIL: EmailStr
    FIRST_SUPERUSER_PASSWORD: str

    # MINIO_ROOT_USER: str
    # MINIO_ROOT_PASSWORD: str
    # MINIO_URL: str
    # MINIO_BUCKET: str

    REDIS_HOST: str = "localhost"
    REDIS_PORT: str = "6379"

    SECRET_KEY: str = "KJMAgRxFdlijZPU8KLLWiJYsebxcDxpTMZDDqGRjJZg"
    ENCRYPT_KEY: str = "q+Y0dzUKGhfDDpAYouIUqLsY/NBIQJ2NMKFWeqjxsk8="


settings = Settings()

