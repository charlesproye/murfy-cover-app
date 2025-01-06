from typing import Optional, Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    Time,
    is_new_value,
)
from ..factory import register_brand, register_merged


class DSDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


class TimerData(msgspec.Struct):
    date: str
    timer_type: str


class Timer(msgspec.Struct):
    data: TimerData
    timestamp: str
    failure: Optional[str] = None


class DSCharging(msgspec.Struct):
    status: Optional[HMApiValue[str]] = None
    estimated_range: Optional[HMApiValue[DataWithUnit[int]]] = None
    time_to_complete_charge: Optional[HMApiValue[DataWithUnit[int]]] = None
    battery_level: Optional[HMApiValue[float]] = None
    plugged_in: Optional[HMApiValue[str]] = None
    battery_capacity: Optional[HMApiValue[DataWithUnit[int]]] = None
    timers: Optional[list[Timer]] = None


@register_brand(rate_limit=36)
class DSInfo(HMApiResponse):
    diagnostics: Optional[DSDiagnostics] = None
    charging: Optional[DSCharging] = None
    brand: str
    vin: str
    request_id: str


class MergedDSDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[DSDiagnostics]) -> Self:
        ret = cls()
        if initial is None:
            return ret
        if initial.odometer is not None:
            ret.odometer = [initial.odometer]
        return ret

    def merge(self, other: Optional[DSDiagnostics]):
        if other is not None:
            if is_new_value(self.odometer, other.odometer):
                self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))


class MergedDSCharging(msgspec.Struct):
    status: list[HMApiValue[str]] = []
    estimated_range: list[HMApiValue[DataWithUnit[int]]] = []
    time_to_complete_charge: list[HMApiValue[DataWithUnit[int]]] = []
    battery_level: list[HMApiValue[float]] = []
    plugged_in: list[HMApiValue[str]] = []
    battery_capacity: list[HMApiValue[DataWithUnit[int]]] = []
    timers: list[Timer] = []  # Modifié: list[list[Timer]] -> list[Timer]

    @classmethod
    def from_initial(cls, initial: Optional[DSCharging]) -> Self:
        ret = cls()
        if initial is not None:
            ret.status = [initial.status] if initial.status is not None else []
            ret.estimated_range = [initial.estimated_range] if initial.estimated_range is not None else []
            ret.time_to_complete_charge = [initial.time_to_complete_charge] if initial.time_to_complete_charge is not None else []
            ret.battery_level = [initial.battery_level] if initial.battery_level is not None else []
            ret.plugged_in = [initial.plugged_in] if initial.plugged_in is not None else []
            ret.battery_capacity = [initial.battery_capacity] if initial.battery_capacity is not None else []
            if initial.timers:  # Modifié
                ret.timers.extend(initial.timers)
        return ret

    def merge(self, other: Optional[DSCharging]):
        if other is not None:
            if is_new_value(self.status, other.status):
                self.status.append(cast(HMApiValue[str], other.status))
            if is_new_value(self.estimated_range, other.estimated_range):
                self.estimated_range.append(cast(HMApiValue[DataWithUnit[int]], other.estimated_range))
            if is_new_value(self.time_to_complete_charge, other.time_to_complete_charge):
                self.time_to_complete_charge.append(cast(HMApiValue[DataWithUnit[int]], other.time_to_complete_charge))
            if is_new_value(self.battery_level, other.battery_level):
                self.battery_level.append(cast(HMApiValue[float], other.battery_level))
            if is_new_value(self.plugged_in, other.plugged_in):
                self.plugged_in.append(cast(HMApiValue[str], other.plugged_in))
            if is_new_value(self.battery_capacity, other.battery_capacity):
                self.battery_capacity.append(cast(HMApiValue[DataWithUnit[int]], other.battery_capacity))
            if other.timers:  # Modifié
                for timer in other.timers:
                    if not any(
                        existing_timer.timestamp == timer.timestamp 
                        for existing_timer in self.timers
                    ):
                        self.timers.append(timer)

@register_merged
class MergedDSInfo(msgspec.Struct):
    diagnostics: MergedDSDiagnostics
    charging: MergedDSCharging
    brand: str
    vin: str
    request_id: str

    @classmethod
    def new(cls) -> Self:
        return cls(
            MergedDSDiagnostics(),
            MergedDSCharging(),
            brand="ds",
            vin="",
            request_id=""
        )

    @classmethod
    def from_initial(cls, initial: DSInfo) -> Self:
        return cls(
            MergedDSDiagnostics.from_initial(initial.diagnostics),
            MergedDSCharging.from_initial(initial.charging),
            brand=initial.brand,
            vin=initial.vin,
            request_id=initial.request_id
        )

    def merge(self, other: DSInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)
        self.brand = other.brand
        self.vin = other.vin
        self.request_id = other.request_id
