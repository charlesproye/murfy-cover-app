from pydantic import BaseModel


class PassportVehicleInfo(BaseModel):
    vin: str
    brand: str
    model: str
    mileage: float | None
    score: str
    start_date: str | None
    image: str | None
    licence_plate: str | None
    warranty_date: float | None
    warranty_km: float | None
    cycles: int


class PassportBatteryInfo(BaseModel):
    oem: str | None
    chemistry: str | None
    capacity: float | None
    range: float
    consumption: float | None
    soh: float | None
    trendline: str | None


class ReportGeneration(BaseModel):
    job_id: str
    message: str
    estimated_duration: str


class PremiumReportData(BaseModel):
    vehicle_info: PassportVehicleInfo
    battery_info: PassportBatteryInfo
    end_of_contract_date: str | None
    last_data_date: str | None
    activation_status: bool | None
    odometer: float | None
    report: ReportGeneration


class PremiumReportPDFUrl(BaseModel):
    job_id: str
    vin: str
    message: str
    url: str | None
    error: str | None
    retry_info: str | None
