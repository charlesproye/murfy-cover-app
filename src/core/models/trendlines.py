from pydantic import BaseModel, Field

from core.models.data_source import DataSource


class Trendlines(BaseModel):
    main: str
    minimum: str
    maximum: str

    source: DataSource = Field(..., description="Source of the trendlines")
