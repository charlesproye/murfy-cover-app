from typing import Annotated, Optional, Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from ..factory import register_brand, register_merged


class BmwDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


class BmwCharging(msgspec.Struct):
    battery_level: Optional[HMApiValue[float]] = None
    status: Optional[HMApiValue[str]] = None


class BmwUsage(msgspec.Struct):
    last_trip_battery_remaining: Optional[
        HMApiValue[Annotated[float, msgspec.Meta(ge=0.0, le=100.0)]]
    ] = None
    last_trip_fuel_consumption: Optional[HMApiValue[DataWithUnit[float]]] = None
    last_trip_energy_consumption: Optional[HMApiValue[DataWithUnit[float]]] = None
    electric_consumption_average: Optional[HMApiValue[DataWithUnit[float]]] = None
    average_weekly_distance: Optional[HMApiValue[DataWithUnit[float]]] = None
    average_weekly_distance_long_run: Optional[HMApiValue[DataWithUnit[float]]] = None


@register_brand(rate_limit=36)
class BmwInfo(HMApiResponse):
    diagnostics: Optional[BmwDiagnostics] = None
    charging: Optional[BmwCharging] = None
    usage: Optional[BmwUsage] = None


class MergedBmwDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[BmwDiagnostics]) -> Self:
        ret = cls()
        if initial is None or initial.odometer is None:
            ret.odometer = []
        else:
            ret.odometer = [initial.odometer]
        return ret

    def merge(self, other: Optional[BmwDiagnostics]):
        if other is not None and is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))


class MergedBmwCharging(msgspec.Struct):
    battery_level: list[HMApiValue[float]] = []
    status: list[HMApiValue[str]] = []

    @classmethod
    def from_initial(cls, initial: Optional[BmwCharging]) -> Self:
        ret = cls()
        if initial is not None:
            if initial.battery_level is not None:
                ret.battery_level = [initial.battery_level]
            if initial.status is not None:
                ret.status = [initial.status]
        return ret

    def merge(self, other: Optional[BmwCharging]):
        if other is not None:
            if is_new_value(self.battery_level, other.battery_level):
                self.battery_level.append(cast(HMApiValue[float], other.battery_level))
            if is_new_value(self.status, other.status):
                self.status.append(cast(HMApiValue[str], other.status))


class MergedBmwUsage(msgspec.Struct):
    last_trip_battery_remaining: list[
        HMApiValue[Annotated[float, msgspec.Meta(ge=0.0, le=100.0)]]
    ] = []
    last_trip_fuel_consumption: list[HMApiValue[DataWithUnit[float]]] = []
    last_trip_energy_consumption: list[HMApiValue[DataWithUnit[float]]] = []
    electric_consumption_average: list[HMApiValue[DataWithUnit[float]]] = []
    average_weekly_distance: list[HMApiValue[DataWithUnit[float]]] = []
    average_weekly_distance_long_run: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[BmwUsage]) -> Self:
        ret = cls()
        if initial is not None:
            if initial.last_trip_battery_remaining is not None:
                ret.last_trip_battery_remaining = [initial.last_trip_battery_remaining]
            if initial.last_trip_fuel_consumption is not None:
                ret.last_trip_fuel_consumption = [initial.last_trip_fuel_consumption]
            if initial.last_trip_energy_consumption is not None:
                ret.last_trip_energy_consumption = [
                    initial.last_trip_energy_consumption
                ]
            if initial.electric_consumption_average is not None:
                ret.electric_consumption_average = [
                    initial.electric_consumption_average
                ]
            if initial.average_weekly_distance is not None:
                ret.average_weekly_distance = [initial.average_weekly_distance]
            if initial.average_weekly_distance_long_run is not None:
                ret.average_weekly_distance_long_run = [
                    initial.average_weekly_distance_long_run
                ]
        return ret

    def merge(self, other: Optional[BmwUsage]):
        if other is not None:
            if is_new_value(
                self.last_trip_battery_remaining, other.last_trip_battery_remaining
            ):
                self.last_trip_battery_remaining.append(
                    cast(HMApiValue[float], other.last_trip_battery_remaining)
                )
            if is_new_value(
                self.last_trip_fuel_consumption, other.last_trip_fuel_consumption
            ):
                self.last_trip_fuel_consumption.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.last_trip_fuel_consumption,
                    )
                )
            if is_new_value(
                self.last_trip_energy_consumption, other.last_trip_energy_consumption
            ):
                self.last_trip_energy_consumption.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.last_trip_energy_consumption,
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
            if is_new_value(
                self.average_weekly_distance, other.average_weekly_distance
            ):
                self.average_weekly_distance.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.average_weekly_distance,
                    )
                )
            if is_new_value(
                self.average_weekly_distance_long_run,
                other.average_weekly_distance_long_run,
            ):
                self.average_weekly_distance_long_run.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.average_weekly_distance_long_run,
                    )
                )


@register_merged
class MergedBmwInfo(msgspec.Struct):
    diagnostics: MergedBmwDiagnostics
    charging: MergedBmwCharging
    usage: MergedBmwUsage

    @classmethod
    def new(cls) -> Self:
        return cls(MergedBmwDiagnostics(), MergedBmwCharging(), MergedBmwUsage())

    @classmethod
    def from_initial(cls, initial: BmwInfo) -> Self:
        return cls(
            MergedBmwDiagnostics.from_initial(initial.diagnostics),
            MergedBmwCharging.from_initial(initial.charging),
            MergedBmwUsage.from_initial(initial.usage),
        )

    def merge(self, other: BmwInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)
        self.usage.merge(other.usage)

