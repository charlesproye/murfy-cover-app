from datetime import date

from pydantic import BaseModel, Field


class PremiumReportGeneration(BaseModel):
    job_id: str
    message: str
    estimated_duration: str


class PremiumReportPDFUrl(BaseModel):
    job_id: str
    vin: str
    message: str
    url: str | None
    error: str | None
    retry_info: str | None


class VehicleInfo(BaseModel):
    vin: str = Field(...)
    brand: str = Field(...)
    model: str = Field(...)
    odometer: float | None = Field(None)
    score: str | None = Field(...)
    start_date: date | None = Field(None)
    image: str | None = Field(None)
    licence_plate: str | None = Field(None)
    warranty_date: int | None = Field(None)
    warranty_km: int | None = Field(None)
    cycles: int | None = Field(None)


class BatteryInfo(BaseModel):
    oem: str | None = Field(None)
    chemistry: str | None = Field(None)
    capacity: float | None = Field(None)
    range: int = Field(...)
    consumption: float | None = Field(None)
    soh: float | None = Field(None)
    trendline: str | None = Field(None)


class PremiumData(BaseModel):
    vehicle_info: VehicleInfo
    battery_info: BatteryInfo
    end_of_contract_date: date | None = Field(None)
    last_data_date: date | None = Field(None)
    activation_status: bool = Field(...)
