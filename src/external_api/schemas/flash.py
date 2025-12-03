import uuid

from pydantic import BaseModel, Field


class SOHWithTrendline(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    trendline_mean: str = Field(..., description="Trendline equation")
    trendline_min: str = Field(..., description="Trendline min")
    trendline_max: str = Field(..., description="Trendline max")
    soh: float = Field(..., description="SOH")
    odometer: int = Field(..., description="Odometer (km)")
