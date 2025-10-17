from pydantic import BaseModel, EmailStr


class FlashReportFormType(BaseModel):
    vin: str
    make: str
    model: str
    type: str
    version: str
    odometer: float
    email: EmailStr

