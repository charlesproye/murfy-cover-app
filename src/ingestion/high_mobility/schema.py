from datetime import datetime
from typing import Generic, Optional, Self, TypeVar, cast

import msgspec

T = TypeVar("T")


class Failure(msgspec.Struct):
    description: str
    reason: str


class HMApiValue(msgspec.Struct, Generic[T]):
    timestamp: datetime
    failure: Optional[Failure]
    data: T


def is_new_value(lst: list[HMApiValue[T]], new: Optional[HMApiValue[T]]) -> bool:
    if new is None:
        return False
    else:
        return new.timestamp not in set(map(lambda o: o.timestamp, lst))


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
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


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


class MercedesBenzInfo(HMApiResponse):
    diagnostics: Optional[MercedesBenzDiagnostics] = None
    charging: Optional[MercedesBenzCharging] = None


class MergedMercedesBenzInfo(msgspec.Struct):
    diagnostics: MergedMercedesBenzDiagnostics
    charging: MergedMercedesBenzCharging

    @classmethod
    def from_initial(cls, initial: MercedesBenzInfo) -> Self:
        return cls(
            MergedMercedesBenzDiagnostics.from_initial(initial.diagnostics),
            MergedMercedesBenzCharging.from_initial(initial.charging),
        )

    def merge(self, other: MercedesBenzInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)


class KiaDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


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


class KiaInfo(HMApiResponse):
    diagnostics: Optional[KiaDiagnostics] = None


class MergedKiaInfo(msgspec.Struct):
    diagnostics: MergedKiaDiagnostics

    @classmethod
    def from_initial(cls, initial: KiaInfo) -> Self:
        return cls(MergedKiaDiagnostics.from_initial(initial.diagnostics))

    def merge(self, other: KiaInfo):
        self.diagnostics.merge(other.diagnostics)


class RenaultDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


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


class RenaultInfo(HMApiResponse):
    diagnostics: Optional[RenaultDiagnostics] = None
    charging: Optional[RenaultCharging] = None


class MergedRenaultInfo(msgspec.Struct):
    diagnostics: MergedRenaultDiagnostics
    charging: MergedRenaultCharging

    @classmethod
    def from_initial(cls, initial: RenaultInfo) -> Self:
        return cls(
            MergedRenaultDiagnostics.from_initial(initial.diagnostics),
            MergedRenaultCharging.from_initial(initial.charging),
        )

    def merge(self, other: RenaultInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)

