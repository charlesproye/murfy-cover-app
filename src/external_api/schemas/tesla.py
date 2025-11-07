from pydantic import BaseModel, EmailStr, Field

from core.tesla.tesla_utils import TeslaRegions


class TeslaCreateUserRequest(BaseModel):
    vin: str
    email: EmailStr = Field(..., description="User email")
    region: TeslaRegions = Field(
        default=TeslaRegions.EUROPE,
        description="User region, used to determine the Tesla API region",
    )
    name: str | None = Field(None, description="Full user name")
