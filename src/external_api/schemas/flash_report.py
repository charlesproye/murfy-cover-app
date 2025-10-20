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
    version: str
    odometer: float
    email: EmailStr
    language: LanguageEnum

