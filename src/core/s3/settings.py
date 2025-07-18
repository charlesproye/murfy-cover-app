from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class S3Settings(BaseSettings):
    S3_REGION: str = Field(default="fr-par")
    S3_ENDPOINT: str = Field(default="https://s3.fr-par.scw.cloud")
    S3_BUCKET: str = Field(default=...)
    S3_KEY: str = Field(default=...)
    S3_SECRET: str = Field(default=...)
    S3_BASE_PATH: str = Field(default=f"s3a://{S3_BUCKET}")

