from typing import Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from ..factory import register_brand, register_merged


class VolvoDiagnostics(msgspec.Struct):
    odometer: HMApiValue[DataWithUnit[float]] | None = None
    distance_since_reset: HMApiValue[DataWithUnit[float]] | None = None
    estimated_range: HMApiValue[DataWithUnit[int]] | None = None
    fuel_volume: HMApiValue[DataWithUnit[float]] | None = None


class VolvoCharging(msgspec.Struct):
    status: HMApiValue[str] | None = None
    estimated_range: HMApiValue[DataWithUnit[int]] | None = None
    time_to_complete_charge: HMApiValue[DataWithUnit[int]] | None = None
    battery_level: HMApiValue[float] | None = None
    plugged_in: HMApiValue[str] | None = None


class VolvoUsage(msgspec.Struct):
    average_speed: HMApiValue[DataWithUnit[float]] | None = None
    average_fuel_consumption: HMApiValue[DataWithUnit[float]] | None = None
    electric_consumption_average: HMApiValue[DataWithUnit[float]] | None = None


# https://docs.high-mobility.com/oem-guides/volvo-cars#refresh-rate
@register_brand(rate_limit=125)
class VolvoInfo(HMApiResponse):
    diagnostics: VolvoDiagnostics | None = None
    charging: VolvoCharging | None = None
    usage: VolvoUsage | None = None


class MergedVolvoDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []
    distance_since_reset: list[HMApiValue[DataWithUnit[float]]] = []
    estimated_range: list[HMApiValue[DataWithUnit[int]]] = []
    fuel_volume: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: VolvoDiagnostics | None) -> Self:
        ret = cls()
        if initial is None:
            return ret
        if initial.odometer is not None:
            ret.odometer = [initial.odometer]
        if initial.distance_since_reset is not None:
            ret.distance_since_reset = [initial.distance_since_reset]
        if initial.estimated_range is not None:
            ret.estimated_range = [initial.estimated_range]
        if initial.fuel_volume is not None:
            ret.fuel_volume = [initial.fuel_volume]
        return ret

    def merge(self, other: VolvoDiagnostics | None):
        if other is not None:
            if is_new_value(self.odometer, other.odometer):
                self.odometer.append(
                    cast(HMApiValue[DataWithUnit[float]], other.odometer)
                )
            if is_new_value(self.distance_since_reset, other.distance_since_reset):
                self.distance_since_reset.append(
                    cast(HMApiValue[DataWithUnit[float]], other.distance_since_reset)
                )
            if is_new_value(self.estimated_range, other.estimated_range):
                self.estimated_range.append(
                    cast(HMApiValue[DataWithUnit[int]], other.estimated_range)
                )
            if is_new_value(self.fuel_volume, other.fuel_volume):
                self.fuel_volume.append(
                    cast(HMApiValue[DataWithUnit[float]], other.fuel_volume)
                )


class MergedVolvoCharging(msgspec.Struct):
    status: list[HMApiValue[str]] = []
    estimated_range: list[HMApiValue[DataWithUnit[int]]] = []
    time_to_complete_charge: list[HMApiValue[DataWithUnit[int]]] = []
    battery_level: list[HMApiValue[float]] = []
    plugged_in: list[HMApiValue[str]] = []

    @classmethod
    def from_initial(cls, initial: VolvoCharging | None) -> Self:
        ret = cls()
        if initial is not None:
            ret.status = [initial.status] if initial.status is not None else []
            ret.estimated_range = (
                [initial.estimated_range] if initial.estimated_range is not None else []
            )
            ret.time_to_complete_charge = (
                [initial.time_to_complete_charge]
                if initial.time_to_complete_charge is not None
                else []
            )
            ret.battery_level = (
                [initial.battery_level] if initial.battery_level is not None else []
            )
            ret.plugged_in = (
                [initial.plugged_in] if initial.plugged_in is not None else []
            )
        return ret

    def merge(self, other: VolvoCharging | None):
        if other is not None:
            if is_new_value(self.status, other.status):
                self.status.append(cast(HMApiValue[str], other.status))
            if is_new_value(self.estimated_range, other.estimated_range):
                self.estimated_range.append(
                    cast(HMApiValue[DataWithUnit[int]], other.estimated_range)
                )
            if is_new_value(
                self.time_to_complete_charge, other.time_to_complete_charge
            ):
                self.time_to_complete_charge.append(
                    cast(HMApiValue[DataWithUnit[int]], other.time_to_complete_charge)
                )
            if is_new_value(self.battery_level, other.battery_level):
                self.battery_level.append(cast(HMApiValue[float], other.battery_level))
            if is_new_value(self.plugged_in, other.plugged_in):
                self.plugged_in.append(cast(HMApiValue[str], other.plugged_in))


class MergedVolvoUsage(msgspec.Struct):
    average_speed: list[HMApiValue[DataWithUnit[float]]] = []
    average_fuel_consumption: list[HMApiValue[DataWithUnit[float]]] = []
    electric_consumption_average: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: VolvoUsage | None) -> Self:
        ret = cls()
        if initial is not None:
            ret.average_speed = (
                [initial.average_speed] if initial.average_speed is not None else []
            )
            ret.average_fuel_consumption = (
                [initial.average_fuel_consumption]
                if initial.average_fuel_consumption is not None
                else []
            )
            ret.electric_consumption_average = (
                [initial.electric_consumption_average]
                if initial.electric_consumption_average is not None
                else []
            )
        return ret

    def merge(self, other: VolvoUsage | None):
        if other is not None:
            if is_new_value(self.average_speed, other.average_speed):
                self.average_speed.append(
                    cast(HMApiValue[DataWithUnit[float]], other.average_speed)
                )
            if is_new_value(
                self.average_fuel_consumption, other.average_fuel_consumption
            ):
                self.average_fuel_consumption.append(
                    cast(
                        HMApiValue[DataWithUnit[float]], other.average_fuel_consumption
                    )
                )
            if is_new_value(
                self.electric_consumption_average, other.electric_consumption_average
            ):
                self.electric_consumption_average.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.electric_consumption_average,
                    )
                )


@register_merged
class MergedVolvoInfo(msgspec.Struct):
    diagnostics: MergedVolvoDiagnostics
    charging: MergedVolvoCharging
    usage: MergedVolvoUsage

    @classmethod
    def new(cls) -> Self:
        return cls(MergedVolvoDiagnostics(), MergedVolvoCharging(), MergedVolvoUsage())

    @classmethod
    def from_initial(cls, initial: VolvoInfo) -> Self:
        return cls(
            MergedVolvoDiagnostics.from_initial(initial.diagnostics),
            MergedVolvoCharging.from_initial(initial.charging),
            MergedVolvoUsage.from_initial(initial.usage),
        )

    def merge(self, other: VolvoInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)
        self.usage.merge(other.usage)

