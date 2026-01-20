from pydantic import BaseModel


class DataPoint(BaseModel):
    soh_bib: float | None
    soh_oem: float | None
    odometer: float


class DataGraphResponse(BaseModel):
    initial_point: DataPoint
    data_points: list[DataPoint]
    trendline_bib: str | None
    trendline_bib_min: str | None
    trendline_bib_max: str | None
    trendline_oem: str | None
    trendline_oem_min: str | None
    trendline_oem_max: str | None
