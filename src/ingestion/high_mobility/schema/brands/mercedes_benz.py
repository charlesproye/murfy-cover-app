from typing import Optional, Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    Time,
    WeekdayTime,
    is_new_value,
)
from ..factory import register_brand, register_merged


class MercedesBenzDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None
    battery_voltage: Optional[HMApiValue[DataWithUnit[float]]] = None
    engine_coolant_temperature: Optional[HMApiValue[DataWithUnit[float]]] = None


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
    
class MercedesBenzUsage(msgspec.Struct):
    electric_consumption_rate_since_reset: Optional[HMApiValue[DataWithUnit[float]]] = None
    electric_consumption_rate_since_start: Optional[HMApiValue[DataWithUnit[float]]] = None
    electric_distance_last_trip: Optional[HMApiValue[DataWithUnit[float]]] = None
    electric_distance_since_reset: Optional[HMApiValue[DataWithUnit[int]]] = None
    electric_duration_last_trip: Optional[HMApiValue[DataWithUnit[int]]] = None
    electric_duration_since_reset: Optional[HMApiValue[DataWithUnit[int]]] = None

class MercedesBenzChargingSession(msgspec.Struct):
    start_time: Optional[HMApiValue[Time]] = None
    displayed_start_state_of_charge: Optional[HMApiValue[float]] = None
    end_time: Optional[HMApiValue[Time]] = None
    displayed_state_of_charge: Optional[HMApiValue[float]] = None
    energy_charged: Optional[HMApiValue[DataWithUnit[float]]] = None
    total_charging_duration: Optional[HMApiValue[DataWithUnit[int]]] = None

@register_brand(rate_limit=36)
class MercedesBenzInfo(HMApiResponse):
    diagnostics: Optional[MercedesBenzDiagnostics] = None
    charging: Optional[MercedesBenzCharging] = None
    usage: Optional[MercedesBenzUsage] = None
    charging_sessions: Optional[list[MercedesBenzChargingSession]] = None

class MergedMercedesBenzDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []
    battery_voltage: list[HMApiValue[DataWithUnit[float]]] = []
    engine_coolant_temperature: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[MercedesBenzDiagnostics]) -> Self:
        ret = cls()
        if initial is None:
            return ret
        if initial.odometer is not None:
            ret.odometer = [initial.odometer]
        if initial.battery_voltage is not None:
            ret.battery_voltage = [initial.battery_voltage]
        if initial.engine_coolant_temperature is not None:
            ret.engine_coolant_temperature = [initial.engine_coolant_temperature]
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

class MergedMercedesBenzUsage(msgspec.Struct):
    electric_consumption_rate_since_reset: list[HMApiValue[DataWithUnit[float]]] = []
    electric_consumption_rate_since_start: list[HMApiValue[DataWithUnit[float]]] = []
    electric_distance_last_trip: list[HMApiValue[DataWithUnit[float]]] = []
    electric_distance_since_reset: list[HMApiValue[DataWithUnit[int]]] = []
    electric_duration_last_trip: list[HMApiValue[DataWithUnit[int]]] = []
    electric_duration_since_reset: list[HMApiValue[DataWithUnit[int]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[MercedesBenzUsage]) -> Self:
        ret = cls()
        if initial is not None:
            ret.electric_consumption_rate_since_reset = (
                [initial.electric_consumption_rate_since_reset] if initial.electric_consumption_rate_since_reset is not None else []
            )
            ret.electric_consumption_rate_since_start = (
                [initial.electric_consumption_rate_since_start]
                if initial.electric_consumption_rate_since_start is not None
                else []
            )
            ret.electric_distance_last_trip = (
                [initial.electric_distance_last_trip] if initial.electric_distance_last_trip is not None else []
            )
            ret.electric_distance_since_reset = (
                [initial.electric_distance_since_reset] if initial.electric_distance_since_reset is not None else []
            )
            ret.electric_duration_last_trip = (
                [initial.electric_duration_last_trip] if initial.electric_duration_last_trip is not None else []
            )
            ret.electric_duration_since_reset = (
                [initial.electric_duration_since_reset] if initial.electric_duration_since_reset is not None else []
            )
        return ret

    def merge(self, other: Optional[MercedesBenzUsage]):
        if other is not None:
            if is_new_value(self.electric_consumption_rate_since_reset, other.electric_consumption_rate_since_reset):
                self.electric_consumption_rate_since_reset.append(
                    cast(HMApiValue[DataWithUnit[float]], other.electric_consumption_rate_since_reset)
                )
            if is_new_value(self.electric_consumption_rate_since_start, other.electric_consumption_rate_since_start):
                self.electric_consumption_rate_since_start.append(
                    cast(HMApiValue[DataWithUnit[float]], other.electric_consumption_rate_since_start)
                )
            if is_new_value(self.electric_distance_last_trip, other.electric_distance_last_trip):
                self.electric_distance_last_trip.append(
                    cast(HMApiValue[DataWithUnit[float]], other.electric_distance_last_trip)
                )
            if is_new_value(self.electric_distance_since_reset, other.electric_distance_since_reset):
                self.electric_distance_since_reset.append(
                    cast(HMApiValue[DataWithUnit[int]], other.electric_distance_since_reset)
                )
            if is_new_value(self.electric_duration_last_trip, other.electric_duration_last_trip):
                self.electric_duration_last_trip.append(
                    cast(HMApiValue[DataWithUnit[int]], other.electric_duration_last_trip)
                )
            if is_new_value(self.electric_duration_since_reset, other.electric_duration_since_reset):
                self.electric_duration_since_reset.append(
                    cast(HMApiValue[DataWithUnit[int]], other.electric_duration_since_reset)
                )

class MergedMercedesBenzChargingSession(msgspec.Struct):
    start_time: list[HMApiValue[Time]] = []
    displayed_start_state_of_charge: list[HMApiValue[float]] = []
    end_time: list[HMApiValue[Time]] = []
    displayed_state_of_charge: list[HMApiValue[float]] = []
    energy_charged: list[HMApiValue[DataWithUnit[float]]] = []
    total_charging_duration: list[HMApiValue[DataWithUnit[int]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[MercedesBenzChargingSession]) -> Self:
        ret = cls()
        if initial is not None:
            ret.start_time = [initial.start_time] if initial.start_time is not None else []
            ret.displayed_start_state_of_charge = (
                [initial.displayed_start_state_of_charge] 
                if initial.displayed_start_state_of_charge is not None 
                else []
            )
            ret.end_time = [initial.end_time] if initial.end_time is not None else []
            ret.displayed_state_of_charge = (
                [initial.displayed_state_of_charge] 
                if initial.displayed_state_of_charge is not None 
                else []
            )
            ret.energy_charged = (
                [initial.energy_charged] if initial.energy_charged is not None else []
            )
            ret.total_charging_duration = (
                [initial.total_charging_duration] 
                if initial.total_charging_duration is not None 
                else []
            )
        return ret

    def merge(self, other: Optional[MercedesBenzChargingSession]):
        if other is not None:
            if is_new_value(self.start_time, other.start_time):
                self.start_time.append(cast(HMApiValue[Time], other.start_time))
            if is_new_value(self.displayed_start_state_of_charge, other.displayed_start_state_of_charge):
                self.displayed_start_state_of_charge.append(
                    cast(HMApiValue[float], other.displayed_start_state_of_charge)
                )
            if is_new_value(self.end_time, other.end_time):
                self.end_time.append(cast(HMApiValue[Time], other.end_time))
            if is_new_value(self.displayed_state_of_charge, other.displayed_state_of_charge):
                self.displayed_state_of_charge.append(
                    cast(HMApiValue[float], other.displayed_state_of_charge)
                )
            if is_new_value(self.energy_charged, other.energy_charged):
                self.energy_charged.append(
                    cast(HMApiValue[DataWithUnit[float]], other.energy_charged)
                )
            if is_new_value(self.total_charging_duration, other.total_charging_duration):
                self.total_charging_duration.append(
                    cast(HMApiValue[DataWithUnit[int]], other.total_charging_duration)
                )

@register_merged
class MergedMercedesBenzInfo(msgspec.Struct):
    diagnostics: MergedMercedesBenzDiagnostics
    charging: MergedMercedesBenzCharging
    usage: MergedMercedesBenzUsage
    charging_sessions: list[MergedMercedesBenzChargingSession] = []

    @classmethod
    def new(cls) -> Self:
        return cls(MergedMercedesBenzDiagnostics(), MergedMercedesBenzCharging(), MergedMercedesBenzUsage(), MergedMercedesBenzChargingSession())

    @classmethod
    def from_initial(cls, initial: MercedesBenzInfo) -> Self:
        return cls(
            MergedMercedesBenzDiagnostics.from_initial(initial.diagnostics),
            MergedMercedesBenzCharging.from_initial(initial.charging),
            MergedMercedesBenzUsage.from_initial(initial.usage),
            MergedMercedesBenzChargingSession.from_initial(initial.charging_sessions),
        )

    def merge(self, other: MercedesBenzInfo):
        self.diagnostics.merge(other.diagnostics)
        self.charging.merge(other.charging)
        self.usage.merge(other.usage)
        self.charging_sessions.merge(other.charging_sessions)
