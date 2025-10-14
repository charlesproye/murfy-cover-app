import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

load_dotenv()


class S3Settings(BaseSettings):
    S3_REGION: str = Field(default="fr-par")
    S3_ENDPOINT: str = Field(default="https://s3.fr-par.scw.cloud")
    S3_BUCKET: str = Field(default=...)
    S3_KEY: str = Field(default=...)
    S3_SECRET: str = Field(default=...)
    S3_BASE_PATH: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        self.S3_BASE_PATH = f"s3a://{self.S3_BUCKET}"


def get_s3_settings(env: str = "prod") -> S3Settings:
    if env == "dev":
        return S3Settings(
            S3_BUCKET=os.getenv("S3_BUCKET_DEV"),
            S3_KEY=os.getenv("S3_KEY_DEV"),
            S3_SECRET=os.getenv("S3_SECRET_DEV"),
            S3_REGION=os.getenv("S3_REGION", "fr-par"),
            S3_ENDPOINT=os.getenv("S3_ENDPOINT_DEV", "https://s3.fr-par.scw.cloud"),
        )
    # Default : prod
    return S3Settings(
        S3_BUCKET=os.getenv("S3_BUCKET"),
        S3_KEY=os.getenv("S3_KEY"),
        S3_SECRET=os.getenv("S3_SECRET"),
        S3_REGION=os.getenv("S3_REGION", "fr-par"),
        S3_ENDPOINT=os.getenv("S3_ENDPOINT", "https://s3.fr-par.scw.cloud"),
    )

