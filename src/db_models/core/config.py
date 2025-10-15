from dotenv import load_dotenv
from pydantic import field_validator
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    WEB_CONCURRENCY: int = 9
    DB_POOL_SIZE: int = 83
    POOL_SIZE: int = max(DB_POOL_SIZE // WEB_CONCURRENCY, 5)
    DB_DATA_EV_USER: str = "data_ev"
    DB_DATA_EV_PASSWORD: str = "data_ev"
    DB_DATA_EV_HOST: str = "localhost"
    DB_DATA_EV_PORT: int = 54323
    DB_DATA_EV_NAME: str = "data_ev"
    ASYNC_DB_DATA_EV_URI: str | None = None

    @field_validator("ASYNC_DB_DATA_EV_URI", mode="before")
    @classmethod
    def assemble_db_DATA_EV_connection(cls, v: str | None, info) -> str:
        if isinstance(v, str):
            return v
        values = info.data
        user = values.get("DB_DATA_EV_USER")
        password = values.get("DB_DATA_EV_PASSWORD")
        host = values.get("DB_DATA_EV_HOST")
        port = values.get("DB_DATA_EV_PORT")
        db_name = values.get("DB_DATA_EV_NAME")
        return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}"

    S3_KEY: str = ""
    S3_SECRET: str = ""
    S3_URL: str = "https://s3.fr-par.scw.cloud"
    S3_BUCKET: str = ""


settings = Settings()

