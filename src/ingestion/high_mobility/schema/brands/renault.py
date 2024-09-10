from typing import Optional, Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from ..factory import register_brand, register_merged


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


@register_brand(rate_limit=36)
class RenaultInfo(HMApiResponse):
    diagnostics: Optional[RenaultDiagnostics] = None
    charging: Optional[RenaultCharging] = None


class MergedRenaultDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[RenaultDiagnostics]) -> Self:
        ret = cls()
        if initial is None or initial.odometer is None:
            ret.odometer = []
        else:
            ret.odometer = [initial.odometer]
        return ret

    def merge(self, other: Optional[RenaultDiagnostics]):
        if other is not None and is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))


class MergedRenaultCharging(msgspec.Struct):
    battery_energy: list[HMApiValue[DataWithUnit[float]]] = []
    battery_level: list[HMApiValue[float]] = []
    charging_rate: list[HMApiValue[DataWithUnit[float]]] = []
    distance_to_complete_charge: list[HMApiValue[DataWithUnit[int]]] = []
    driving_mode_phev: list[HMApiValue[str]] = []
    estimated_range: list[HMApiValue[DataWithUnit[int]]] = []
    plugged_in: list[HMApiValue[str]] = []
    battery_charge_type: list[HMApiValue[str]] = []
    status: list[HMApiValue[str]] = []

    @classmethod
    def from_initial(cls, initial: Optional[RenaultCharging]) -> Self:
        ret = cls()
        if initial is not None:
            if initial.battery_energy is not None:
                ret.battery_energy = [initial.battery_energy]
            if initial.battery_level is not None:
                ret.battery_level = [initial.battery_level]
            if initial.charging_rate is not None:
                ret.charging_rate = [initial.charging_rate]
            if initial.distance_to_complete_charge is not None:
                ret.distance_to_complete_charge = [initial.distance_to_complete_charge]
            if initial.driving_mode_phev is not None:
                ret.driving_mode_phev = [initial.driving_mode_phev]
            if initial.estimated_range is not None:
                ret.estimated_range = [initial.estimated_range]
            if initial.plugged_in is not None:
                ret.plugged_in = [initial.plugged_in]
            if initial.battery_charge_type is not None:
                ret.battery_charge_type = [initial.battery_charge_type]
            if initial.status is not None:
                ret.status = [initial.status]
        return ret

    def merge(self, other: Optional[RenaultCharging]):
        if other is not None:
            if is_new_value(self.battery_energy, other.battery_energy):
                self.battery_energy.append(
                    cast(HMApiValue[DataWithUnit[float]], other.battery_energy)
                )
            if is_new_value(self.battery_level, other.battery_level):
                self.battery_level.append(cast(HMApiValue[float], other.battery_level))
            if is_new_value(self.charging_rate, other.charging_rate):
                self.charging_rate.append(
                    cast(HMApiValue[DataWithUnit[float]], other.charging_rate)
                )
            if is_new_value(
                self.distance_to_complete_charge, other.distance_to_complete_charge
            ):
                self.distance_to_complete_charge.append(
                    cast(
                        HMApiValue[DataWithUnit[int]], other.distance_to_complete_charge
                    )
                )
            if is_new_value(self.driving_mode_phev, other.driving_mode_phev):
                self.driving_mode_phev.append(
                    cast(HMApiValue[str], other.driving_mode_phev)
                )
            if is_new_value(self.estimated_range, other.estimated_range):
                self.estimated_range.append(
                    cast(HMApiValue[DataWithUnit[int]], other.estimated_range)
                )
            if is_new_value(self.plugged_in, other.plugged_in):
                self.plugged_in.append(cast(HMApiValue[str], other.plugged_in))
            if is_new_value(self.battery_charge_type, other.battery_charge_type):
                self.battery_charge_type.append(
                    cast(HMApiValue[str], other.battery_charge_type)
                )
            if is_new_value(self.status, other.status):
                self.status.append(cast(HMApiValue[str], other.status))


@register_merged
class MergedRenaultInfo(msgspec.Struct):
    diagnostics: MergedRenaultDiagnostics
    charging: MergedRenaultCharging

    @classmethod
    def new(cls) -> Self:
        return cls(MergedRenaultDiagnostics(), MergedRenaultCharging())

    @classmethod
    def from_initial(cls, initial: RenaultInfo) -> Self:
        return cls(
            MergedRenaultDiagnostics.from_initial(initial.diagnostics),
            MergedRenaultCharging.from_initial(initial.charging),
        )

    def merge(self, other: RenaultInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)

