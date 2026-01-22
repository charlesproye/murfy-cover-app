from pydantic import BaseModel, EmailStr

from db_models.enums import LanguageEnum


class FlashReportFormType(BaseModel):
    vin: str
    make: str
    model: str
    type: str
    version: str | None = None
    odometer: int
    email: EmailStr
    language: LanguageEnum


class VehicleInfo(BaseModel):
    vin: str
    brand: str
    model: str
    type: str | None
    version: str | None
    mileage: float
    image_url: str | None
    warranty_date: float | None
    warranty_km: float | None


class BatteryInfo(BaseModel):
    oem: str | None
    chemistry: str | None
    net_capacity: float
    capacity: float
    consumption: float | None
    range: float
    trendline_bib: str | None
    trendline_bib_min: str | None
    trendline_bib_max: str | None
    trendline_oem: str | None
    trendline_oem_min: str | None
    trendline_oem_max: str | None
    soh_bib: float | None
    soh_oem: float | None


class GenerationData(BaseModel):
    has_trendline: bool
    language: LanguageEnum
    vehicle_info: VehicleInfo
    battery_info: BatteryInfo


class VehicleSpecsType(BaseModel):
    type: str
    version: str


class VehicleSpecs(BaseModel):
    has_trendline: bool
    has_trendline_bib: bool
    has_trendline_oem: bool
    make: str | None
    model: str | None
    type_version_list: list[VehicleSpecsType] | None
