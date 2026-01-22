import uuid
from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator, model_validator


class ActivationRequestVehicle(BaseModel):
    vin: str = Field(..., min_length=17, max_length=17)
    make: str
    model: str | None = None
    type: str | None = None  # EV, PHEV, ICE, etc.


class Activation(BaseModel):
    request_soh_bib: bool
    request_soh_oem: bool
    start_date: date | None = None
    end_date: date | None = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            # Try multiple date formats
            date_formats = [
                "%Y-%m-%d",  # ISO format: 2025-12-24
                "%d/%m/%Y",  # European format: 24/12/2025
                "%d/%m/%y",  # European short year: 24/12/25
                "%m/%d/%Y",  # US format: 12/24/2025
                "%m/%d/%y",  # US short year: 12/24/25
            ]

            # Try ISO datetime first
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
            except ValueError:
                pass

            # Try each format
            for fmt in date_formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            # If nothing worked, raise an error
            raise ValueError(
                f"Invalid date format: {v}. Expected formats: YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY"
            )
        return v

    @model_validator(mode="after")
    def check_dates(self):
        if (
            self.start_date
            and self.end_date
            and isinstance(self.start_date, date)
            and isinstance(self.end_date, date)
            and self.end_date <= self.start_date
        ):
            raise ValueError("end_date must be after start_date")
        return self


class ActivationOrder(BaseModel):
    vehicle: ActivationRequestVehicle
    activation: Activation | None = None
    comment: str | None = Field(None, max_length=500)


class ActivationRequest(BaseModel):
    fleet_id: str
    activation_orders: list[ActivationOrder] = Field(..., min_items=1)


class VehicleStatus(BaseModel):
    vin: str = Field(..., min_length=17, max_length=17)
    requested_soh_readout: bool
    requested_soh_bib: bool
    requested_activation: bool
    status: bool | None = None
    message: str | None = None
    consent_information: str | None = None
    comment: str | None = None


class ActivationResponse(BaseModel):
    vehicles: list[VehicleStatus]


class DeactivationRequest(BaseModel):
    fleet_id: str
    vins: list[str] = Field(..., min_items=1)


class DeactivationResponse(BaseModel):
    vehicles: list[VehicleStatus]


class FleetInfo(BaseModel):
    fleet_id: str
    fleet_name: str


class FleetInfoResponse(BaseModel):
    fleets: list[FleetInfo]


class MakeInfo(BaseModel):
    make_id: str
    make_name: str
    make_conditions: str | None = None
    soh_readout: bool = False
    soh_bib: bool = False


class MakeInfoResponse(BaseModel):
    makes: list[MakeInfo]


class ModelInfo(BaseModel):
    model_id: str
    model_name: str
    type: str | None = None
    version: str | None = None
    battery_capacity: float | None = None


class ModelInfoResponse(BaseModel):
    models: list[ModelInfo]


class ModelType(BaseModel):
    model_id: uuid.UUID = Field(..., description="Model ID")
    make: str = Field(..., description="Make (company that made the car)")
    model_name: str = Field(..., description="Model name")
    model_type: str | None = Field(None, description="Model type")
    commissioning_date: datetime | None = Field(..., description="Commissioning date")
    end_of_life_date: datetime | None = Field(..., description="End of life date")
    has_flash_soh: bool = Field(
        ..., description="SoH can be statistically estimated on this model type"
    )
    has_oem_soh: bool = Field(
        ..., description="SoH readout is available for this model type"
    )
    has_bib_soh: bool = Field(
        ..., description="SoH can be calculated by BIB for this model type"
    )
