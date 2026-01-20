"""Schemas for vehicle related data"""

from typing import Any

from pydantic import BaseModel, Field


class ModelTrendlineData(BaseModel):
    """Trendline information"""

    # id: UUID4 = Field(..., description="ID unique of the model")
    model_name: str = Field(..., description="Name of the model of the vehicle")
    trendline_mean: dict[str, Any] | None = Field(
        None, description="Trendline mean", nullable=True
    )
    trendline_min: dict[str, Any] | None = Field(
        None, description="Trendline min", nullable=True
    )
    trendline_max: dict[str, Any] | None = Field(
        None, description="Trendline max", nullable=True
    )
    comment: str | None = Field(None, description="Comment")

    model_config = {"from_attributes": True, "protected_namespaces": ()}


class ModelWarrantyData(BaseModel):
    """Trendline information"""

    # id: UUID4 = Field(..., description="ID unique of the model")
    model_name: str = Field(..., description="Name of the model of the vehicle")
    warranty_km: int | None = Field(None, description="Warranty in km")
    warranty_date: int | None = Field(None, description="Warranty in years")

    class Config:
        from_attributes = True
