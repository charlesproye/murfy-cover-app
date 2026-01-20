import uuid

from pydantic import BaseModel, Field


class SOHWithTrendline(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    trendline_bib_min: list[tuple[int, float]] | None = Field(
        ...,
        description="Coordinates from the BIB lower trendline to help draw the curve",
        nullable=True,
    )
    trendline_bib_max: list[tuple[int, float]] | None = Field(
        ...,
        description="Coordinates from the BIB upper trendline to help draw the curve",
        nullable=True,
    )
    trendline_oem_min: list[tuple[int, float]] | None = Field(
        None,
        description="Coordinates from the OEM lower trendline to help draw the curve",
        nullable=True,
    )
    trendline_oem_max: list[tuple[int, float]] | None = Field(
        None,
        description="Coordinates from the OEM upper trendline to help draw the curve",
        nullable=True,
    )
    soh_bib: float | None = Field(None, description="SoH BIB estimated", nullable=True)
    soh_oem: float | None = Field(None, description="SoH OEM estimated", nullable=True)
    odometer: int = Field(..., description="Odometer (km)")
