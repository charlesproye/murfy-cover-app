from typing import Optional, Self, cast

import msgspec

from transform.compressor.providers.schemas.utils import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from transform.compressor.providers.schemas.factory import register_brand, register_merged


class KiaDiagnostics(msgspec.Struct):
    battery_level: Optional[HMApiValue[float]] = None  # Ajouté pour correspondre au JSON
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None
    estimated_mixed_powertrain_range: Optional[HMApiValue[DataWithUnit[float]]] = None


class KiaCharging(msgspec.Struct):
    battery_level: Optional[HMApiValue[float]] = None
    charge_port_state: Optional[HMApiValue[str]] = None  # Ajouté pour correspondre au JSON
    estimated_range: Optional[HMApiValue[DataWithUnit[float]]] = None
    plugged_in: Optional[HMApiValue[str]] = None
    preconditioning_immediate_status: Optional[HMApiValue[str]] = None  # Ajouté pour correspondre au JSON


@register_brand(rate_limit=36)
class KiaInfo(HMApiResponse):
    diagnostics: Optional[KiaDiagnostics] = None
    charging: Optional[KiaCharging] = None


class MergedKiaDiagnostics(msgspec.Struct):
    battery_level: list[HMApiValue[float]] = []  # Ajouté pour correspondre au JSON
    odometer: list[HMApiValue[DataWithUnit[float]]] = []
    estimated_mixed_powertrain_range: list[HMApiValue[DataWithUnit[float]]] = []


class MergedKiaCharging(msgspec.Struct):
    battery_level: list[HMApiValue[float]] = []
    charge_port_state: list[HMApiValue[str]] = []  # Ajouté pour correspondre au JSON
    estimated_range: list[HMApiValue[DataWithUnit[float]]] = []
    plugged_in: list[HMApiValue[str]] = []
    preconditioning_immediate_status: list[HMApiValue[str]] = []  # Ajouté pour correspondre au JSON


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
        if is_new_value(self.battery_level, other.battery_level):
            self.battery_level.append(cast(HMApiValue[float], other.battery_level))
        if is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))
        if is_new_value(self.estimated_mixed_powertrain_range, other.estimated_mixed_powertrain_range):
            self.estimated_mixed_powertrain_range.append(cast(HMApiValue[DataWithUnit[float]], other.estimated_mixed_powertrain_range))

def merge_charging(self, other: Optional[KiaCharging]):
    if other is not None:
        if is_new_value(self.battery_level, other.battery_level):
            self.battery_level.append(cast(HMApiValue[float], other.battery_level))
        if is_new_value(self.charge_port_state, other.charge_port_state):
            self.charge_port_state.append(cast(HMApiValue[str], other.charge_port_state))
        if is_new_value(self.estimated_range, other.estimated_range):
            self.estimated_range.append(cast(HMApiValue[DataWithUnit[float]], other.estimated_range))
        if is_new_value(self.plugged_in, other.plugged_in):
            self.plugged_in.append(cast(HMApiValue[str], other.plugged_in))
        if is_new_value(self.preconditioning_immediate_status, other.preconditioning_immediate_status):
            self.preconditioning_immediate_status.append(cast(HMApiValue[str], other.preconditioning_immediate_status))

MergedKiaDiagnostics.merge = merge_diagnostics
MergedKiaCharging.merge = merge_charging
