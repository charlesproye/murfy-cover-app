from typing import Self, cast

import msgspec

from .. import DataWithUnit, HMApiResponse, HMApiValue, is_new_value
from ..factory import register_brand, register_merged


class FordDiagnostics(msgspec.Struct):
    odometer: HMApiValue[DataWithUnit[float]] | None = None
    fuel_level: HMApiValue[DataWithUnit[float]] | None = None
    speed: HMApiValue[DataWithUnit[float]] | None = None
    engine_coolant_temperature: HMApiValue[DataWithUnit[float]] | None = None


class FordCharging(msgspec.Struct):
    battery_energy: HMApiValue[DataWithUnit[float]] | None = None
    battery_level: HMApiValue[float] | None = None
    charge_limit: HMApiValue[float] | None = None
    charger_voltage: HMApiValue[DataWithUnit[float]] | None = None
    charging_current: HMApiValue[DataWithUnit[float]] | None = None
    time_to_complete_charge: HMApiValue[DataWithUnit[float]] | None = None  # Changed
    status: HMApiValue[str] | None = None
    battery_performance_status: HMApiValue[str] | None = None


class FordUsage(msgspec.Struct):
    last_trip_battery_regenerated: HMApiValue[DataWithUnit[float]] | None = None
    electric_distance_last_trip: HMApiValue[DataWithUnit[float]] | None = None


# https://docs.high-mobility.com/oem-guides/ford#data-package-update-frequency
@register_brand(rate_limit=35)
class FordInfo(HMApiResponse):
    diagnostics: FordDiagnostics | None = None
    charging: FordCharging | None = None
    usage: FordUsage | None = None


class MergedFordDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []
    fuel_level: list[HMApiValue[DataWithUnit[float]]] = []
    speed: list[HMApiValue[DataWithUnit[float]]] = []
    engine_coolant_temperature: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: FordDiagnostics | None) -> Self:
        ret = cls()
        if initial is not None:
            if initial.odometer is not None:
                ret.odometer = [initial.odometer]
            if initial.fuel_level is not None:
                ret.fuel_level = [initial.fuel_level]
            if initial.speed is not None:
                ret.speed = [initial.speed]
            if initial.engine_coolant_temperature is not None:
                ret.engine_coolant_temperature = [initial.engine_coolant_temperature]
        return ret

    def merge(self, other: FordDiagnostics | None):
        if other is not None:
            if is_new_value(self.odometer, other.odometer):
                self.odometer.append(
                    cast(HMApiValue[DataWithUnit[float]], other.odometer)
                )
            if is_new_value(self.fuel_level, other.fuel_level):
                self.fuel_level.append(
                    cast(HMApiValue[DataWithUnit[float]], other.fuel_level)
                )
            if is_new_value(self.speed, other.speed):
                self.speed.append(cast(HMApiValue[DataWithUnit[float]], other.speed))
            if is_new_value(
                self.engine_coolant_temperature, other.engine_coolant_temperature
            ):
                self.engine_coolant_temperature.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.engine_coolant_temperature,
                    )
                )


class MergedFordCharging(msgspec.Struct):
    battery_energy: list[HMApiValue[DataWithUnit[float]]] = []
    battery_level: list[HMApiValue[float]] = []
    charge_limit: list[HMApiValue[float]] = []
    charger_voltage: list[HMApiValue[DataWithUnit[float]]] = []
    charging_current: list[HMApiValue[DataWithUnit[float]]] = []
    time_to_complete_charge: list[HMApiValue[DataWithUnit[float]]] = []  # Changed
    status: list[HMApiValue[str]] = []
    battery_performance_status: list[HMApiValue[str]] = []

    @classmethod
    def from_initial(cls, initial: FordCharging | None) -> Self:
        ret = cls()
        if initial is not None:
            if initial.battery_energy is not None:
                ret.battery_energy = [initial.battery_energy]
            if initial.battery_level is not None:
                ret.battery_level = [initial.battery_level]
            if initial.charge_limit is not None:
                ret.charge_limit = [initial.charge_limit]
            if initial.charger_voltage is not None:
                ret.charger_voltage = [initial.charger_voltage]
            if initial.status is not None:
                ret.status = [initial.status]
            if initial.time_to_complete_charge is not None:
                ret.time_to_complete_charge = [initial.time_to_complete_charge]
            if initial.battery_performance_status is not None:
                ret.battery_performance_status = [initial.battery_performance_status]
        return ret

    def merge(self, other: FordCharging | None):
        if other is not None:
            if is_new_value(self.battery_level, other.battery_level):
                self.battery_level.append(cast(HMApiValue[float], other.battery_level))
            if is_new_value(self.status, other.status):
                self.status.append(cast(HMApiValue[str], other.status))
            if is_new_value(self.battery_energy, other.battery_energy):
                self.battery_energy.append(
                    cast(HMApiValue[DataWithUnit[float]], other.battery_energy)
                )
            if is_new_value(self.charger_voltage, other.charger_voltage):
                self.charger_voltage.append(
                    cast(HMApiValue[float], other.charger_voltage)
                )
            if is_new_value(
                self.time_to_complete_charge, other.time_to_complete_charge
            ):
                self.time_to_complete_charge.append(
                    cast(HMApiValue[DataWithUnit[float]], other.time_to_complete_charge)
                )  # Changed
            if is_new_value(
                self.battery_performance_status, other.battery_performance_status
            ):
                self.battery_performance_status.append(
                    cast(HMApiValue[str], other.battery_performance_status)
                )


class MergedFordUsage(msgspec.Struct):
    last_trip_battery_regenerated: list[HMApiValue[DataWithUnit[float]]] = []
    electric_distance_last_trip: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: FordUsage | None) -> Self:
        ret = cls()
        if initial is not None:
            if initial.last_trip_battery_regenerated is not None:
                ret.last_trip_battery_regenerated = [
                    initial.last_trip_battery_regenerated
                ]
            if initial.electric_distance_last_trip is not None:
                ret.electric_distance_last_trip = [initial.electric_distance_last_trip]
        return ret

    def merge(self, other: FordUsage | None):
        if other is not None:
            if is_new_value(
                self.last_trip_battery_regenerated, other.last_trip_battery_regenerated
            ):
                self.last_trip_battery_regenerated.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.last_trip_battery_regenerated,
                    )
                )
            if is_new_value(
                self.electric_distance_last_trip, other.electric_distance_last_trip
            ):
                self.electric_distance_last_trip.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.electric_distance_last_trip,
                    )
                )


@register_merged
class MergedFordInfo(msgspec.Struct):
    diagnostics: MergedFordDiagnostics
    charging: MergedFordCharging
    usage: MergedFordUsage

    @classmethod
    def new(cls) -> Self:
        return cls(MergedFordDiagnostics(), MergedFordCharging(), MergedFordUsage())

    @classmethod
    def from_initial(cls, initial: FordInfo) -> Self:
        return cls(
            MergedFordDiagnostics.from_initial(initial.diagnostics),
            MergedFordCharging.from_initial(initial.charging),
            MergedFordUsage.from_initial(initial.usage),
        )

    def merge(self, other: FordInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)
        self.usage.merge(other.usage)

