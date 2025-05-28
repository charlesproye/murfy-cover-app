from pydantic import BaseModel


class VehicleConfirmation(BaseModel):
    vin: str
    verification_code: str

