from typing import Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from ..factory import register_brand, register_merged


class RenaultDiagnostics(msgspec.Struct):
    odometer: HMApiValue[DataWithUnit[float]] | None = None
    estimated_range: HMApiValue[DataWithUnit[int]] | None = None
    speed: HMApiValue[DataWithUnit[float]] | None = None


class RenaultCharging(msgspec.Struct):
    battery_energy: HMApiValue[DataWithUnit[float]] | None = None
    battery_level: HMApiValue[float] | None = None
    charging_rate: HMApiValue[DataWithUnit[float]] | None = None
    distance_to_complete_charge: HMApiValue[DataWithUnit[int]] | None = None
    driving_mode_phev: HMApiValue[str] | None = None
    estimated_range: HMApiValue[DataWithUnit[int]] | None = None
    plugged_in: HMApiValue[str] | None = None
    battery_charge_type: HMApiValue[str] | None = None
    status: HMApiValue[str] | None = None


class RenaultClimate(msgspec.Struct):
    outside_temperature: HMApiValue[DataWithUnit[float]] | None = None


class RenaultEngine(msgspec.Struct):
    status: HMApiValue[DataWithUnit[float]] | None = None


class RenaultOffroad(msgspec.Struct):
    route_incline: HMApiValue[DataWithUnit[float]] | None = None


class RenaultRace(msgspec.Struct):
    accelerations: HMApiValue[DataWithUnit[float]] | None = None
    acceleration_duration: HMApiValue[DataWithUnit[float]] | None = None


class RenaultUsage(msgspec.Struct):
    last_trip_battery_remaining: HMApiValue[DataWithUnit[float]] | None = None
    last_trip_energy_consumption: HMApiValue[DataWithUnit[float]] | None = None


# https://docs.high-mobility.com/oem-guides/renault-and-dacia#data-package-sampling-frequency
@register_brand(rate_limit=35)
class RenaultInfo(HMApiResponse):
    diagnostics: RenaultDiagnostics | None = None
    charging: RenaultCharging | None = None
    climate: RenaultClimate | None = None
    engine: RenaultEngine | None = None
    offroad: RenaultOffroad | None = None
    race: RenaultRace | None = None
    usage: RenaultUsage | None = None


class MergedRenaultDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []
    estimated_range: list[HMApiValue[DataWithUnit[int]]] = []
    speed: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: RenaultDiagnostics | None) -> Self:
        ret = cls()
        if initial is not None:
            if initial.odometer is not None:
                ret.odometer = [initial.odometer]
            if initial.estimated_range is not None:
                ret.estimated_range = [initial.estimated_range]
            if initial.speed is not None:
                ret.speed = [initial.speed]
        return ret

    def merge(self, other: RenaultDiagnostics | None):
        if other is not None and is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))
        if other is not None and is_new_value(
            self.estimated_range, other.estimated_range
        ):
            self.estimated_range.append(
                cast(HMApiValue[DataWithUnit[int]], other.estimated_range)
            )
        if other is not None and is_new_value(self.speed, other.speed):
            self.speed.append(cast(HMApiValue[DataWithUnit[float]], other.speed))


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
    def from_initial(cls, initial: RenaultCharging | None) -> Self:
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

    def merge(self, other: RenaultCharging | None):
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


class MergedRenaultClimate(msgspec.Struct):
    outside_temperature: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: RenaultClimate | None) -> Self:
        ret = cls()
        if initial is None or initial.outside_temperature is None:
            ret.outside_temperature = []
        else:
            ret.outside_temperature = [initial.outside_temperature]
        return ret

    def merge(self, other: RenaultClimate | None):
        if other is not None and is_new_value(
            self.outside_temperature, other.outside_temperature
        ):
            self.outside_temperature.append(
                cast(HMApiValue[DataWithUnit[float]], other.outside_temperature)
            )


class MergedRenaultEngine(msgspec.Struct):
    status: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: RenaultEngine | None) -> Self:
        ret = cls()
        if initial is None or initial.status is None:
            ret.status = []
        else:
            ret.status = [initial.status]
        return ret

    def merge(self, other: RenaultEngine | None):
        if other is not None and is_new_value(self.status, other.status):
            self.status.append(cast(HMApiValue[DataWithUnit[float]], other.status))


class MergedRenaultOffroad(msgspec.Struct):
    route_incline: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: RenaultOffroad | None) -> Self:
        ret = cls()
        if initial is None or initial.route_incline is None:
            ret.route_incline = []
        else:
            ret.route_incline = [initial.route_incline]
        return ret

    def merge(self, other: RenaultOffroad | None):
        if other is not None and is_new_value(self.route_incline, other.route_incline):
            self.route_incline.append(
                cast(HMApiValue[DataWithUnit[float]], other.route_incline)
            )


class MergedRenaultRace(msgspec.Struct):
    accelerations: list[HMApiValue[DataWithUnit[float]]] = []
    acceleration_duration: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: RenaultRace | None) -> Self:
        ret = cls()
        if initial is not None:
            if initial.accelerations is not None:
                ret.accelerations = [initial.accelerations]
            if initial.acceleration_duration is not None:
                ret.acceleration_duration = [initial.acceleration_duration]
        return ret

    def merge(self, other: RenaultRace | None):
        if other is not None:
            if is_new_value(self.accelerations, other.accelerations):
                self.accelerations.append(
                    cast(HMApiValue[DataWithUnit[float]], other.accelerations)
                )
            if is_new_value(self.acceleration_duration, other.acceleration_duration):
                self.acceleration_duration.append(
                    cast(HMApiValue[DataWithUnit[float]], other.acceleration_duration)
                )


class MergedRenaultUsage(msgspec.Struct):
    last_trip_battery_remaining: list[HMApiValue[DataWithUnit[float]]] = []
    last_trip_energy_consumption: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: RenaultUsage | None) -> Self:
        ret = cls()
        if initial is not None:
            if initial.last_trip_battery_remaining is not None:
                ret.last_trip_battery_remaining = [initial.last_trip_battery_remaining]
            if initial.last_trip_energy_consumption is not None:
                ret.last_trip_energy_consumption = [
                    initial.last_trip_energy_consumption
                ]
        return ret

    def merge(self, other: RenaultUsage | None):
        if other is not None:
            if is_new_value(
                self.last_trip_battery_remaining, other.last_trip_battery_remaining
            ):
                self.last_trip_battery_remaining.append(
                    cast(
                        HMApiValue[DataWithUnit[float]],
                        other.last_trip_battery_remaining,
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


@register_merged
class MergedRenaultInfo(msgspec.Struct):
    diagnostics: MergedRenaultDiagnostics
    charging: MergedRenaultCharging
    climate: MergedRenaultClimate
    engine: MergedRenaultEngine
    offroad: MergedRenaultOffroad
    race: MergedRenaultRace
    usage: MergedRenaultUsage

    @classmethod
    def new(cls) -> Self:
        return cls(
            MergedRenaultDiagnostics(),
            MergedRenaultCharging(),
            MergedRenaultClimate(),
            MergedRenaultEngine(),
            MergedRenaultOffroad(),
            MergedRenaultRace(),
            MergedRenaultUsage(),
        )

    @classmethod
    def from_initial(cls, initial: RenaultInfo) -> Self:
        return cls(
            MergedRenaultDiagnostics.from_initial(initial.diagnostics),
            MergedRenaultCharging.from_initial(initial.charging),
            MergedRenaultClimate.from_initial(initial.climate),
            MergedRenaultEngine.from_initial(initial.engine),
            MergedRenaultOffroad.from_initial(initial.offroad),
            MergedRenaultRace.from_initial(initial.race),
            MergedRenaultUsage.from_initial(initial.usage),
        )

    def merge(self, other: RenaultInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)
        self.climate.merge(other.climate)
        self.engine.merge(other.engine)
        self.offroad.merge(other.offroad)
        self.race.merge(other.race)
        self.usage.merge(other.usage)

