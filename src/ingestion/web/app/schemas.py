from datetime import datetime
from pydantic import BaseModel, Field


class Trip(BaseModel):
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


class Maintenance(BaseModel):
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


class Location(BaseModel):
    event: str
    locationId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    latitude: float | None = None
    longitude: float | None = None


class CruisingRange(BaseModel):
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


class DashboardErrorWarning(BaseModel):
    event: str
    dashboardErrorWarningId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    odometer: int | None = None
    warningDescription: str | None = None
    warningId: str | None = None
    warningCategory: str | None = None


class EnergyLevel(BaseModel):
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


class ChargingState(BaseModel):
    event: str
    chargingStateId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    targetSoc: int | None = None


class ChargingRemainingTime(BaseModel):
    event: str
    chargingRemainingTimeId: str | None = None
    vin: str | None = None
    vehicleTimeUtc: datetime | None = None
    vehicleTimeLocal: datetime | None = None
    remainingTimeMinutes: int | None = None

