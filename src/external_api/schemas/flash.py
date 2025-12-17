import uuid

from pydantic import BaseModel, Field


class SOHWithTrendline(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    trendline_min: list[tuple[int, float]] = Field(
        ...,
        description="Coordinates from the lower trendline to help draw the curve",
    )
    trendline_max: list[tuple[int, float]] = Field(
        ...,
        description="Coordinates from the upper trendline to help draw the curve",
    )
    soh: float = Field(..., description="SO estimated")
    odometer: int = Field(..., description="Odometer (km)")
