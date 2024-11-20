from typing import Optional, Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from ..factory import register_brand, register_merged


class KiaDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None
    estimated_mixed_powertrain_range: Optional[HMApiValue[DataWithUnit[float]]] = None


class KiaCharging(msgspec.Struct):
    battery_level: Optional[HMApiValue[float]] = None
    estimated_range: Optional[HMApiValue[DataWithUnit[float]]] = None
    plugged_in: Optional[HMApiValue[str]] = None


@register_brand(rate_limit=36)
class KiaInfo(HMApiResponse):
    diagnostics: Optional[KiaDiagnostics] = None
    charging: Optional[KiaCharging] = None


class MergedKiaDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []
    estimated_mixed_powertrain_range: list[HMApiValue[DataWithUnit[float]]] = []


class MergedKiaCharging(msgspec.Struct):
    battery_level: list[HMApiValue[float]] = []
    estimated_range: list[HMApiValue[DataWithUnit[float]]] = []
    plugged_in: list[HMApiValue[str]] = []


@register_merged
class MergedKiaInfo(msgspec.Struct):
    diagnostics: MergedKiaDiagnostics
    charging: MergedKiaCharging

    @classmethod
    def new(cls) -> Self:
        return cls(MergedKiaDiagnostics(), MergedKiaCharging())

    @classmethod
    def from_initial(cls, initial: KiaInfo) -> Self:
        return cls(
            MergedKiaDiagnostics.from_initial(initial.diagnostics),
            MergedKiaCharging.from_initial(initial.charging)
        )

    def merge(self, other: KiaInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)


# Add methods to MergedKiaDiagnostics and MergedKiaCharging to handle merging
def merge_diagnostics(self, other: Optional[KiaDiagnostics]):
    if other is not None:
        if is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))
        if is_new_value(self.estimated_mixed_powertrain_range, other.estimated_mixed_powertrain_range):
            self.estimated_mixed_powertrain_range.append(cast(HMApiValue[DataWithUnit[float]], other.estimated_mixed_powertrain_range))

def merge_charging(self, other: Optional[KiaCharging]):
    if other is not None:
        if is_new_value(self.battery_level, other.battery_level):
            self.battery_level.append(cast(HMApiValue[float], other.battery_level))
        if is_new_value(self.estimated_range, other.estimated_range):
            self.estimated_range.append(cast(HMApiValue[DataWithUnit[float]], other.estimated_range))
        if is_new_value(self.plugged_in, other.plugged_in):
            self.plugged_in.append(cast(HMApiValue[str], other.plugged_in))

MergedKiaDiagnostics.merge = merge_diagnostics
MergedKiaCharging.merge = merge_charging
