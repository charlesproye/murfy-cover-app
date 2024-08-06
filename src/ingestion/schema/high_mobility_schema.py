from datetime import datetime
from typing import Generic, Optional, TypeVar

import msgspec

T = TypeVar("T")


class Failure(msgspec.Struct):
    description: str
    reason: str


class HMApiValue(msgspec.Struct, Generic[T]):
    timestamp: datetime
    failure: Optional[Failure]
    data: T


class HMApiResponse(msgspec.Struct):
    brand: str
    vin: str
    request_id: str


class DataWithUnit(msgspec.Struct, Generic[T]):
    unit: str
    value: T


class Time(msgspec.Struct):
    hour: int
    minute: int


class WeekdayTime(msgspec.Struct):
    time: Time
    weekday: str


class MercedesBenzDiagnostics(msgspec.Struct):
    odometer: HMApiValue[DataWithUnit[float]]


class MercedesBenzCharging(msgspec.Struct):
    battery_level: Optional[HMApiValue[float]] = None
    battery_level_at_departure: Optional[HMApiValue[float]] = None
    charging_rate: Optional[HMApiValue[DataWithUnit[float]]] = None
    estimated_range: Optional[HMApiValue[DataWithUnit[int]]] = None
    max_range: Optional[HMApiValue[DataWithUnit[int]]] = None
    plugged_in: Optional[HMApiValue[str]] = None
    fully_charged_end_times: Optional[HMApiValue[WeekdayTime]] = None
    preconditioning_scheduled_time: Optional[HMApiValue[Time]] = None
    preconditioning_remaining_time: Optional[HMApiValue[DataWithUnit[int]]] = None
    preconditioning_departure_status: Optional[HMApiValue[str]] = None
    smart_charging_status: Optional[HMApiValue[str]] = None
    starter_battery_state: Optional[HMApiValue[str]] = None
    status: Optional[HMApiValue[str]] = None
    time_to_complete_charge: Optional[HMApiValue[float]] = None


class MercedesBenzInfo(HMApiResponse):
    diagnostics: Optional[MercedesBenzDiagnostics] = None
    charging: Optional[MercedesBenzCharging] = None


class KiaDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


class KiaInfo(HMApiResponse):
    diagnostics: Optional[KiaDiagnostics] = None


class RenaultDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


class RenaultCharging(msgspec.Struct):
    battery_energy: Optional[HMApiValue[DataWithUnit[float]]] = None
    battery_level: Optional[HMApiValue[float]] = None
    charging_rate: Optional[HMApiValue[DataWithUnit[float]]] = None
    distance_to_complete_charge: Optional[HMApiValue[DataWithUnit[int]]] = None
    driving_mode_phev: Optional[HMApiValue[str]] = None
    estimated_range: Optional[HMApiValue[DataWithUnit[int]]] = None
    plugged_in: Optional[HMApiValue[str]] = None
    battery_charge_type: Optional[HMApiValue[str]] = None
    status: Optional[HMApiValue[str]] = None


class RenaultInfo(HMApiResponse):
    diagnostics: Optional[RenaultDiagnostics] = None
    charging: Optional[RenaultCharging] = None

