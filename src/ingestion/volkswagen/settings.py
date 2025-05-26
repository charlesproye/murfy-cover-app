from pydantic import Field
from pydantic_settings import BaseSettings


class VolksWagenSettings(BaseSettings):
    VW_ORGANIZATION_ID: str = Field(default=...)
    VW_USERNAME: str = Field(default=...)
    VW_PASSWORD: str = Field(default=...)

