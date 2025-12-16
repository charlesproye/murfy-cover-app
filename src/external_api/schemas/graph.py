from pydantic import BaseModel


class DataPoint(BaseModel):
    soh: float
    odometer: float


class DataGraphResponse(BaseModel):
    initial_point: DataPoint
    data_points: list[DataPoint]
    trendline: str | None
    trendline_min: str | None
    trendline_max: str | None
