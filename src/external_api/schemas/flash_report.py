from enum import Enum

from pydantic import BaseModel, EmailStr


class LanguageEnum(str, Enum):
    en = "en"
    fr = "fr"


class FlashReportFormType(BaseModel):
    vin: str
    make: str
    model: str
    type: str
    version: str | None = None
    odometer: float
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
    trendline: str | None
    trendline_min: str | None
    trendline_max: str | None
    soh: float | None


class GenerationData(BaseModel):
    has_trendline: bool
    language: LanguageEnum
    vehicle_info: VehicleInfo
    battery_info: BatteryInfo


class VehicleSpecs(BaseModel):
    has_trendline: bool
    make: str
    model: str
    type: str | None
    version: str | None

