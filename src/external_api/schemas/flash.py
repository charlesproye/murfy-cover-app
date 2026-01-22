import uuid

from pydantic import BaseModel, Field

from core.models.data_source import DataSource


class SOHWithTrendline(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    trendline_min: list[tuple[int, float]] | None = Field(
        ...,
        description="Coordinates from the lower trendline to help draw the curve",
        nullable=True,
    )
    trendline_max: list[tuple[int, float]] | None = Field(
        ...,
        description="Coordinates from the upper trendline to help draw the curve",
        nullable=True,
    )
    soh: float | None = Field(
        None, description="Estimated State of Health", nullable=True
    )
    source: DataSource = Field(..., description="Source of the SoH")
    odometer: int = Field(..., description="Odometer (km)")
