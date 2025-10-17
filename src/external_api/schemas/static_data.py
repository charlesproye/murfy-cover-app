from datetime import datetime

from pydantic import BaseModel, Field


class TypeInfo(BaseModel):
    model_type: str = Field(..., description="Model type")
    versions: list[str] = Field(..., description="Versions")


class ModelInfo(BaseModel):
    model_name: str = Field(..., description="Model name")
    types: list[TypeInfo] = Field(..., description="Types")


class MakeInfo(BaseModel):
    make_name: str = Field(..., description="Make name")
    models: list[ModelInfo] = Field(..., description="Models")


class AllMakesModelsInfo(BaseModel):
    makes: list[MakeInfo] = Field(..., description="Makes")


class ModelType(BaseModel):
    model_name: str = Field(..., description="Model name")
    model_type: str = Field(..., description="Model type")
    commissioning_date: datetime | None = Field(..., description="Commissioning date")
    end_of_life_date: datetime | None = Field(..., description="End of life date")


class ModelTrendline(BaseModel):
    model_name: str = Field(..., description="Model name")
    trendline_mean: str = Field(..., description="Trendline equation")
    trendline_min: str = Field(..., description="Trendline min")
    trendline_max: str = Field(..., description="Trendline max")
    commissioning_date: datetime | None = Field(..., description="Commissioning date")
    end_of_life_date: datetime | None = Field(..., description="End of life date")
    comment: str | None = Field(..., description="Comment")


class SOHWithTrendline(BaseModel):
    trendline_mean: str = Field(..., description="Trendline equation")
    trendline_min: str = Field(..., description="Trendline min")
    trendline_max: str = Field(..., description="Trendline max")
    soh: float = Field(..., description="SOH")
    odometer: int = Field(..., description="Odometer (km)")
    commissioning_date: datetime | None = Field(..., description="Commissioning date")
    end_of_life_date: datetime | None = Field(..., description="End of life date")

