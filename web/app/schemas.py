from datetime import datetime

from pydantic import BaseModel, Field


class BaseModelWithVin(BaseModel):
    vin: str | None = None
    received_date: datetime = Field(default_factory=datetime.now)


class Trip(BaseModelWithVin):
    event: str  # = Field(..., pattern="^TRIP$", example="TRIP")
    tripId: str | None = None
    vin: str | None = None
    drivenDistance: int | None = None
    duration: int | None = None
    startTimeUtc: datetime | None = None
    startTimeLocal: datetime | None = None
    endTimeUtc: datetime | None = None
    endTimeLocal: datetime | None = None
    endOdometer: int | None = None
    startLatitude: float | None = None
    startLongitude: float | None = None
    endLatitude: float | None = None
    endLongitude: float | None = None


class Maintenance(BaseModelWithVin):
    event: str  # = Field(..., pattern="^MAINTENANCE$", example="MAINTENANCE")
    maintenanceId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    nextOilChangeDistance: int | None = None
    nextOilChangeTime: int | None = None
    nextServiceDistance: int | None = None
    nextServiceTime: int | None = None


class Location(BaseModelWithVin):
    event: str
    locationId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    latitude: float | None = None
    longitude: float | None = None


class CruisingRange(BaseModelWithVin):
    event: str
    cruisingRangeId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    cruisingRangeTotal: int | None = None
    cruisingRangeAdblue: int | None = None
    cruisingRangePrimaryEngine: int | None = None
    cruisingRangeSecondaryEngine: int | None = None


class DashboardErrorWarning(BaseModelWithVin):
    event: str
    dashboardErrorWarningId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    warningDescription: str | None = None
    warningId: str | None = None
    warningCategory: str | None = None


class EnergyLevel(BaseModelWithVin):
    event: str
    energyLevelId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    energyLevelPrimaryEngine: int | None = None
    energyLevelPrimaryEnginePercentage: int | None = None
    energyLevelSecondaryEngine: int | None = None
    energyLevelSecondaryEnginePercentage: int | None = None


class ChargingState(BaseModelWithVin):
    event: str
    chargingStateId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    targetSoc: int | None = None


class ChargingRemainingTime(BaseModelWithVin):
    event: str
    chargingRemainingTimeId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    remainingTimeMinutes: int | None = None


class PushKeyValue(BaseModel):
    key: str
    value: str
    unit: str | None = None
    info: str | None = None
    date_of_value: datetime


class VehiclePushKeyValues(BaseModelWithVin):
    vin: str | None = None
    pushKeyValues: list[PushKeyValue]


class PushDataRequest(BaseModel):
    vehiclePushKeyValues: list[VehiclePushKeyValues]

