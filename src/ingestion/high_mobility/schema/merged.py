from typing import Annotated, Optional, Protocol, Self, TypeVar, cast

import msgspec
from ingestion.high_mobility.schema import (
    BmwCharging,
    BmwDiagnostics,
    BmwInfo,
    BmwUsage,
    DataWithUnit,
    HMApiValue,
    KiaDiagnostics,
    KiaInfo,
    MercedesBenzCharging,
    MercedesBenzDiagnostics,
    MercedesBenzInfo,
    RenaultCharging,
    RenaultDiagnostics,
    RenaultInfo,
    Time,
    WeekdayTime,
    is_new_value,
)



class MergedMercedesBenzDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[MercedesBenzDiagnostics]) -> Self:
        ret = cls()
        if initial is None or initial.odometer is None:
            ret.odometer = []
        else:
            ret.odometer = [initial.odometer]
        return ret

    def merge(self, other: Optional[MercedesBenzDiagnostics]):
        if other is not None and is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))


class MergedMercedesBenzCharging(msgspec.Struct):
    battery_level: list[HMApiValue[float]] = []
    battery_level_at_departure: list[HMApiValue[float]] = []
    charging_rate: list[HMApiValue[DataWithUnit[float]]] = []
    estimated_range: list[HMApiValue[DataWithUnit[int]]] = []
    max_range: list[HMApiValue[DataWithUnit[int]]] = []
    plugged_in: list[HMApiValue[str]] = []
    fully_charged_end_times: list[HMApiValue[WeekdayTime]] = []
    preconditioning_scheduled_time: list[HMApiValue[Time]] = []
    preconditioning_remaining_time: list[HMApiValue[DataWithUnit[int]]] = []
    preconditioning_departure_status: list[HMApiValue[str]] = []
    smart_charging_status: list[HMApiValue[str]] = []
    starter_battery_state: list[HMApiValue[str]] = []
    status: list[HMApiValue[str]] = []
    time_to_complete_charge: list[HMApiValue[float]] = []

    @classmethod
    def from_initial(cls, initial: Optional[MercedesBenzCharging]) -> Self:
        ret = cls()
        if initial is not None:
            ret.battery_level = (
                [initial.battery_level] if initial.battery_level is not None else []
            )
            ret.battery_level_at_departure = (
                [initial.battery_level_at_departure]
                if initial.battery_level_at_departure is not None
                else []
            )
            ret.charging_rate = (
                [initial.charging_rate] if initial.charging_rate is not None else []
            )
            ret.estimated_range = (
                [initial.estimated_range] if initial.estimated_range is not None else []
            )
            ret.max_range = [initial.max_range] if initial.max_range is not None else []
            ret.plugged_in = (
                [initial.plugged_in] if initial.plugged_in is not None else []
            )
            ret.fully_charged_end_times = (
                [initial.fully_charged_end_times]
                if initial.fully_charged_end_times is not None
                else []
            )
            ret.preconditioning_scheduled_time = (
                [initial.preconditioning_scheduled_time]
                if initial.preconditioning_scheduled_time is not None
                else []
            )
            ret.preconditioning_remaining_time = (
                [initial.preconditioning_remaining_time]
                if initial.preconditioning_remaining_time is not None
                else []
            )
            ret.preconditioning_departure_status = (
                [initial.preconditioning_departure_status]
                if initial.preconditioning_departure_status is not None
                else []
            )
            ret.smart_charging_status = (
                [initial.smart_charging_status]
                if initial.smart_charging_status is not None
                else []
            )
            ret.starter_battery_state = (
                [initial.starter_battery_state]
                if initial.starter_battery_state is not None
                else []
            )
            ret.status = [initial.status] if initial.status is not None else []
            ret.time_to_complete_charge = (
                [initial.time_to_complete_charge]
                if initial.time_to_complete_charge is not None
                else []
            )
        return ret

    def merge(self, other: Optional[MercedesBenzCharging]):
        if other is not None:
            if is_new_value(self.battery_level, other.battery_level):
                self.battery_level.append(cast(HMApiValue[float], other.battery_level))
            if is_new_value(
                self.battery_level_at_departure, other.battery_level_at_departure
            ):
                self.battery_level_at_departure.append(
                    cast(HMApiValue[float], other.battery_level_at_departure)
                )
            if is_new_value(self.charging_rate, other.charging_rate):
                self.charging_rate.append(
                    cast(HMApiValue[DataWithUnit[float]], other.charging_rate)
                )
            if is_new_value(self.estimated_range, other.estimated_range):
                self.estimated_range.append(
                    cast(HMApiValue[DataWithUnit[int]], other.estimated_range)
                )
            if is_new_value(self.max_range, other.max_range):
                self.max_range.append(
                    cast(HMApiValue[DataWithUnit[int]], other.max_range)
                )
            if is_new_value(self.plugged_in, other.plugged_in):
                self.plugged_in.append(cast(HMApiValue[str], other.plugged_in))
            if is_new_value(
                self.fully_charged_end_times, other.fully_charged_end_times
            ):
                self.fully_charged_end_times.append(
                    cast(HMApiValue[WeekdayTime], other.fully_charged_end_times)
                )
            if is_new_value(
                self.preconditioning_scheduled_time,
                other.preconditioning_scheduled_time,
            ):
                self.preconditioning_scheduled_time.append(
                    cast(HMApiValue[Time], other.preconditioning_scheduled_time)
                )
            if is_new_value(
                self.preconditioning_remaining_time,
                other.preconditioning_remaining_time,
            ):
                self.preconditioning_remaining_time.append(
                    cast(
                        HMApiValue[DataWithUnit[int]],
                        other.preconditioning_remaining_time,
                    )
                )
            if is_new_value(
                self.preconditioning_departure_status,
                other.preconditioning_departure_status,
            ):
                self.preconditioning_departure_status.append(
                    cast(HMApiValue[str], other.preconditioning_departure_status)
                )
            if is_new_value(self.smart_charging_status, other.smart_charging_status):
                self.smart_charging_status.append(
                    cast(HMApiValue[str], other.smart_charging_status)
                )
            if is_new_value(self.starter_battery_state, other.starter_battery_state):
                self.starter_battery_state.append(
                    cast(HMApiValue[str], other.starter_battery_state)
                )
            if is_new_value(self.status, other.status):
                self.status.append(cast(HMApiValue[str], other.status))
            if is_new_value(
                self.time_to_complete_charge, other.time_to_complete_charge
            ):
                self.time_to_complete_charge.append(
                    cast(HMApiValue[float], other.time_to_complete_charge)
                )


class MergedMercedesBenzInfo(msgspec.Struct):
    diagnostics: MergedMercedesBenzDiagnostics
    charging: MergedMercedesBenzCharging

    @classmethod
    def new(cls) -> Self:
        return cls(MergedMercedesBenzDiagnostics(), MergedMercedesBenzCharging())

    @classmethod
    def from_initial(cls, initial: MercedesBenzInfo) -> Self:
        return cls(
            MergedMercedesBenzDiagnostics.from_initial(initial.diagnostics),
            MergedMercedesBenzCharging.from_initial(initial.charging),
        )

    def merge(self, other: MercedesBenzInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)


class MergedKiaDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[KiaDiagnostics]) -> Self:
        ret = cls()
        if initial is None or initial.odometer is None:
            ret.odometer = []
        else:
            ret.odometer = [initial.odometer]
        return ret

    def merge(self, other: Optional[KiaDiagnostics]):
        if other is not None and is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))


class MergedKiaInfo(msgspec.Struct):
    diagnostics: MergedKiaDiagnostics

    @classmethod
    def new(cls) -> Self:
        return cls(MergedKiaDiagnostics())

    @classmethod
    def from_initial(cls, initial: KiaInfo) -> Self:
        return cls(MergedKiaDiagnostics.from_initial(initial.diagnostics))

    def merge(self, other: KiaInfo):
        self.diagnostics.merge(other.diagnostics)


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

