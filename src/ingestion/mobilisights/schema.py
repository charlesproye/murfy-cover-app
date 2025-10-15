import datetime
from collections.abc import Iterable
from datetime import datetime as dt
from enum import StrEnum
from typing import Annotated, Generic, Self, TypeVar

import msgspec

T = TypeVar("T")
U = TypeVar("U")


class WithTimestamp(
    msgspec.Struct,
    forbid_unknown_fields=False,
    omit_defaults=True,
    rename="camel",
    kw_only=True,
):
    datetime: dt | None = None


class TimestampedValue(WithTimestamp, Generic[T]):
    value: T

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class ValueWithUnit(
    msgspec.Struct,
    Generic[T, U],
    forbid_unknown_fields=False,
    omit_defaults=True,
    rename="camel",
    kw_only=True,
):
    value: T | None = None
    unit: U | None = None


class TimestampedValueWithUnit(WithTimestamp, Generic[T, U]):
    value: T
    unit: U

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if isinstance(el, list):
                # Si c'est une liste, on l'aplatit
                res.extend([x for x in el if x is not None])
            elif el is not None and el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class Percentage(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    percentage: str | float
    datetime: dt | None = None

    def __post_init__(self):
        self.percentage = float(self.percentage)

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class DurationUnit(StrEnum):
    milliseconds = "ms"
    seconds = "s"
    minutes = "min"
    hours = "h"
    days = "d"
    weeks = "w"
    months = "m"
    years = "y"


class Duration(msgspec.Struct):
    value: float
    unit: DurationUnit


class AzimuthUnit(StrEnum):
    degrees = "°"


class GpsSource(StrEnum):
    dead_reckoning = "dead-reckoning"
    real_gps = "real-gps"
    no_gps = "no-gps"


class DistanceUnit(StrEnum):
    meters = "m"
    kilometers = "km"
    miles = "mi"


class Geolocation(WithTimestamp, forbid_unknown_fields=False):
    latitude: float | None = None
    longitude: float | None = None
    source: GpsSource | None = None
    gps_signal: Annotated[float, msgspec.Meta(ge=0, le=100)] | None = None
    altitude: TimestampedValueWithUnit[float, DistanceUnit] | None = None

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []

        for el in lst:
            if isinstance(el, list):
                # Si c'est une liste, on l'aplatit
                for item in el:
                    if (
                        item
                        and hasattr(item, "datetime")
                        and item.datetime not in {e.datetime for e in res}
                    ):
                        res.append(item)
            elif (
                el
                and hasattr(el, "datetime")
                and el.datetime not in {e.datetime for e in res}
            ):
                res.append(el)
        return res


class SpeedUnit(StrEnum):
    kilometers_per_hour = "km/h"
    miles_per_hour = "mi/h"


class VehicleStatus(StrEnum):
    driving = "driving"
    halted = "halted"
    idling = "idling"
    parked = "parked"
    starting = "starting"
    towed_away = "towed-away"


class AccelerationUnit(StrEnum):
    meters_per_second_squared = "m/s²"


class FuelConsumptionUnit(StrEnum):
    liters_per_100_kilometers = "L/100 km"


class VolumeUnit(StrEnum):
    liters = "L"


class FuelConsumptionLevel(WithTimestamp):
    percentage: str | float
    unit: VolumeUnit | None = None
    value: float | None = None

    def __post_init__(self):
        self.percentage = float(self.percentage)

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class EngineStatus(StrEnum):
    off = "off"
    starting = "starting"
    running = "running"
    start_and_stop = "start-and-stop"


class EngineSpeedUnit(StrEnum):
    revolutions_per_minute = "rpm"


class EngineSmall(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    status: TimestampedValue[EngineStatus] | None = None
    speed: TimestampedValueWithUnit[float, EngineSpeedUnit] | None = None


class Fuel(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    average_consumption: TimestampedValueWithUnit[float, FuelConsumptionUnit] | None = (
        None
    )
    instant_consumption: TimestampedValueWithUnit[float, FuelConsumptionUnit] | None = (
        None
    )
    total_consumption: TimestampedValueWithUnit[float, VolumeUnit] | None = None
    level: FuelConsumptionLevel | None = None
    residual_autonomy: TimestampedValueWithUnit[float, DistanceUnit] | None = None
    engine: EngineSmall | None = None


class MergedFuel(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    average_consumption: list[TimestampedValueWithUnit[float, FuelConsumptionUnit]] = []
    instant_consumption: list[TimestampedValueWithUnit[float, FuelConsumptionUnit]] = []
    total_consumption: list[TimestampedValueWithUnit[float, VolumeUnit]] = []
    level: list[FuelConsumptionLevel] = []
    residual_autonomy: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    engine_speed: list[TimestampedValueWithUnit[float, EngineSpeedUnit]] = []

    @classmethod
    def from_list(cls, lst: list[Fuel]) -> Self:
        res = cls()
        res.average_consumption = TimestampedValueWithUnit.merge_list(
            [x for x in (e.average_consumption for e in lst) if x is not None]
        )
        res.instant_consumption = TimestampedValueWithUnit.merge_list(
            [x for x in (e.instant_consumption for e in lst) if x is not None]
        )
        res.total_consumption = TimestampedValueWithUnit.merge_list(
            [x for x in (e.total_consumption for e in lst) if x is not None]
        )
        res.level = FuelConsumptionLevel.merge_list(
            [x for x in (e.level for e in lst) if x is not None]
        )
        res.residual_autonomy = TimestampedValueWithUnit.merge_list(
            [x for x in (e.residual_autonomy for e in lst) if x is not None]
        )
        res.engine_speed = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.engine.speed if e.engine else None for e in lst)
                if x is not None
            ]
        )
        return res


class EnergyConsumptionUnit(StrEnum):
    kilowatthours_per_100_kilometers = "kWh/100 km"


class EnergyUnit(StrEnum):
    killowatt_hours = "kWh"


class EnergyConsumptionLevel(WithTimestamp):
    value: float | None = None  # Rendre 'value' optionnel
    unit: EnergyUnit | None = None  # Rendre 'unit' optionnel
    percentage: str | float | None = None  # Rendre 'percentage' optionnel

    def __post_init__(self):
        if self.percentage is not None:
            self.percentage = float(self.percentage)

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class CapacityUnit(StrEnum):
    watt_hours = "Wh"


class ChargingStatus(StrEnum):
    disconnected = "disconnected"
    in_progress = "in-progress"
    failure = "failure"
    stopped = "stopped"
    finished = "finished"


class ChargingRateUnit(StrEnum):
    kilometers_per_hour = "km/h"


class ChargingMode(StrEnum):
    slow = "slow"
    quick = "quick"
    no = "no"


class Charging(WithTimestamp, forbid_unknown_fields=False):
    plugged: bool | None = None
    status: ChargingStatus | None = None
    remainingTime: ValueWithUnit[int, DurationUnit] | None = (
        None  # Changed to ValueWithUnit instead of TimestampedValueWithUnit
    )
    mode: ChargingMode | None = None
    planned: dt | None = None
    rate: int | ValueWithUnit[float, ChargingRateUnit] | None = None

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class Battery(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    stateOfHealth: Percentage | None = None


class MergedBattery(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    stateOfHealth: list[Percentage] = []

    @classmethod
    def from_list(cls, lst: list[Battery]) -> Self | None:
        if not lst:
            return None
        res = cls()
        res.stateOfHealth = [x for x in (e.stateOfHealth for e in lst) if x is not None]
        return res


class Electricity(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    capacity: TimestampedValueWithUnit[float, CapacityUnit] | None = None
    charging: Charging | None = None
    engine: EngineSmall | None = None
    residual_autonomy: TimestampedValueWithUnit[float, DistanceUnit] | None = None
    level: EnergyConsumptionLevel | None = None
    instant_consumption: (
        TimestampedValueWithUnit[float, EnergyConsumptionUnit] | None
    ) = None
    battery: Battery | None = None

    @classmethod
    def __struct_from_dict__(cls, d):
        if "capacity" in d:
            d["battery_capacity"] = d.pop("capacity")
        return super().__struct_from_dict__(d)


class MergedElectricity(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    instant_consumption: list[
        TimestampedValueWithUnit[float, EnergyConsumptionUnit]
    ] = []
    level: list[EnergyConsumptionLevel] = []
    residual_autonomy: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    battery_capacity: list[TimestampedValueWithUnit[float, CapacityUnit]] = []
    charging: list[Charging] = []
    engine_speed: list[TimestampedValueWithUnit[float, EngineSpeedUnit]] = []
    battery: MergedBattery | None = None

    @classmethod
    def from_list(cls, lst: list[Electricity]) -> Self:
        res = cls()
        res.instant_consumption = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (getattr(e, "instant_consumption", None) for e in lst)
                if x is not None
            ]
        )
        res.level = EnergyConsumptionLevel.merge_list(
            [x for x in (e.level for e in lst) if x is not None]
        )
        res.residual_autonomy = TimestampedValueWithUnit.merge_list(
            [x for x in (e.residual_autonomy for e in lst) if x is not None]
        )
        res.battery_capacity = TimestampedValueWithUnit.merge_list(
            [x for x in (e.capacity for e in lst) if x is not None]
        )
        res.charging = Charging.merge_list(
            [x for x in (e.charging for e in lst) if x is not None]
        )
        res.engine_speed = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.engine.speed if e.engine else None for e in lst)
                if x is not None
            ]
        )
        res.battery = MergedBattery.from_list(
            [x for x in (e.battery for e in lst) if x is not None]
        )
        return res


class TemperatureUnit(StrEnum):
    celcius_degrees = "°C"


class PressureUnit(StrEnum):
    bar = "bar"
    psi = "psi"


class EngineOil(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    temperature: TimestampedValueWithUnit[float, TemperatureUnit] | None = None
    pressure: TimestampedValueWithUnit[float, PressureUnit] | None = None
    life_left: Percentage | None = None


class VoltageUnit(StrEnum):
    volts = "V"


class EngineBattery(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    capacity: Percentage | None = None
    resistance: object | None = None
    voltage: TimestampedValueWithUnit[float, VoltageUnit] | None = None
    datetime: dt | None = None

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in {e.datetime for e in res}:
                res.append(el)
        return res


class EngineCoolant(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    temperature: TimestampedValueWithUnit[float, TemperatureUnit]


class Engine(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    oil: EngineOil | None = None
    contact: TimestampedValue[bool] | None = None
    status: TimestampedValue[EngineStatus] | None = None
    speed: TimestampedValueWithUnit[float, EngineSpeedUnit] | None = None
    ignition: TimestampedValue[bool] | None = None
    battery: EngineBattery | None = None
    percentage: Percentage | None = None
    run_time: TimestampedValue[float] | None = None
    coolant: EngineCoolant | None = None


class MergedEngine(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    oil_temperature: list[TimestampedValueWithUnit[float, TemperatureUnit]] = []
    oil_pressure: list[TimestampedValueWithUnit[float, PressureUnit]] = []
    contact: list[TimestampedValue[bool]] = []
    status: list[TimestampedValue[EngineStatus]] = []
    speed: list[TimestampedValueWithUnit[float, EngineSpeedUnit]] = []
    ignition: list[TimestampedValue[bool]] = []
    battery: list[EngineBattery] = []
    run_time: list[TimestampedValue[float]] = []
    coolant_temperature: list[TimestampedValueWithUnit[float, TemperatureUnit]] = []

    @classmethod
    def from_list(cls, lst: list[Engine]) -> Self:
        res = cls()
        res.oil_temperature = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.oil.temperature if e.oil else None for e in lst)
                if x is not None
            ]
        )
        res.oil_pressure = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.oil.pressure if e.oil else None for e in lst)
                if x is not None
            ]
        )
        res.contact = TimestampedValue.merge_list(
            [x for x in (e.contact for e in lst) if x is not None]
        )
        res.status = TimestampedValue.merge_list(
            [x for x in (e.status for e in lst) if x is not None]
        )
        res.speed = TimestampedValueWithUnit.merge_list(
            [x for x in (e.speed for e in lst) if x is not None]
        )
        res.ignition = TimestampedValue.merge_list(
            [x for x in (e.ignition for e in lst) if x is not None]
        )
        res.battery = EngineBattery.merge_list(
            [x for x in (e.battery for e in lst) if x is not None]
        )
        res.run_time = TimestampedValue.merge_list(
            [x for x in (e.run_time for e in lst) if x is not None]
        )
        res.coolant_temperature = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.coolant.temperature if e.coolant else None for e in lst)
                if x is not None
            ]
        )
        return res


class ParkAssistValue(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    alarm: TimestampedValue[bool] | None = None
    muted: TimestampedValue[bool] | None = None


class MergedParkAssistValue(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    alarm: list[TimestampedValue[bool]] = []
    muted: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[ParkAssistValue]) -> Self:
        res = cls()
        res.alarm = TimestampedValue.merge_list(
            [x for x in (e.alarm for e in lst) if x is not None]
        )
        res.muted = TimestampedValue.merge_list(
            [x for x in (e.muted for e in lst) if x is not None]
        )
        return res


class ParkAssist(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: ParkAssistValue
    rear: ParkAssistValue


class MergedParkAssist(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    front: MergedParkAssistValue | None = None
    rear: MergedParkAssistValue | None = None

    @classmethod
    def from_list(cls, lst: list[ParkAssist]) -> Self:
        res = cls()
        res.front = MergedParkAssistValue.from_list(
            [x for x in (e.front for e in lst) if x is not None]
        )
        res.rear = MergedParkAssistValue.from_list(
            [x for x in (e.rear for e in lst) if x is not None]
        )
        return res


class KeepAssist(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    right: TimestampedValue[bool] | None = None
    left: TimestampedValue[bool] | None = None


class Lane(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    keep_assist: KeepAssist | None = None


class MergedLaneKeepAssist(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    right: list[TimestampedValue[bool]] = []
    left: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Lane]) -> Self:
        res = cls()
        res.left = TimestampedValue.merge_list(
            [
                x
                for x in (e.keep_assist.left if e.keep_assist else None for e in lst)
                if x is not None
            ]
        )
        res.right = TimestampedValue.merge_list(
            [
                x
                for x in (e.keep_assist.right if e.keep_assist else None for e in lst)
                if x is not None
            ]
        )
        return res


class Adas(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    park_assist: ParkAssist | None = None
    lane: Lane | None = None
    esp: TimestampedValue[bool] | None = None
    abs: TimestampedValue[bool] | None = None
    blind_spot_monitoring: TimestampedValue[bool] | None = None
    fse: TimestampedValue[bool] | None = None
    sli: TimestampedValueWithUnit[float, SpeedUnit] | None = None


class MergedAdas(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    park_assist: MergedParkAssist | None = None
    lane_keep_assist: MergedLaneKeepAssist | None = None
    esp: list[TimestampedValue[bool]] = []
    abs: list[TimestampedValue[bool]] = []
    blind_spot_monitoring: list[TimestampedValue[bool]] = []
    fse: list[TimestampedValue[bool]] = []
    sli: list[TimestampedValueWithUnit[float, SpeedUnit]] = []

    @classmethod
    def from_list(cls, lst: list[Adas]) -> Self:
        res = cls()
        res.park_assist = MergedParkAssist.from_list(
            [x for x in (e.park_assist for e in lst) if x is not None]
        )
        res.lane_keep_assist = MergedLaneKeepAssist.from_list(
            [x for x in (e.lane for e in lst) if x is not None]
        )
        res.esp = TimestampedValue.merge_list(
            [x for x in (e.esp for e in lst) if x is not None]
        )
        res.abs = TimestampedValue.merge_list(
            [x for x in (e.abs for e in lst) if x is not None]
        )
        res.blind_spot_monitoring = TimestampedValue.merge_list(
            [x for x in (e.blind_spot_monitoring for e in lst) if x is not None]
        )
        res.fse = TimestampedValue.merge_list(
            [x for x in (e.fse for e in lst) if x is not None]
        )
        res.sli = TimestampedValueWithUnit.merge_list(
            [x for x in (e.sli for e in lst) if x is not None]
        )
        return res


class FogLights(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: TimestampedValue[bool] | None = None
    rear: TimestampedValue[bool] | None = None


class Turn(StrEnum):
    left = "left"
    right = "right"


class Lights(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    fog: FogLights | None = None
    turn: TimestampedValue[Turn] | None = None
    warnings: TimestampedValue[bool] | None = None


class MergedLights(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    fog_front: list[TimestampedValue[bool]] = []
    fog_rear: list[TimestampedValue[bool]] = []
    turn: list[TimestampedValue[Turn]] = []
    warnings: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Lights]) -> Self:
        res = cls()
        res.fog_front = TimestampedValue.merge_list(
            [x for x in (e.fog.front if e.fog else None for e in lst) if x is not None]
        )
        res.fog_rear = TimestampedValue.merge_list(
            [x for x in (e.fog.rear if e.fog else None for e in lst) if x is not None]
        )
        res.turn = TimestampedValue.merge_list(
            [x for x in (e.turn for e in lst) if x is not None]
        )
        res.warnings = TimestampedValue.merge_list(
            [x for x in (e.warnings for e in lst) if x is not None]
        )
        return res


class RearPassengerSeatbelt(msgspec.Struct, forbid_unknown_fields=False):
    left: TimestampedValue[bool] | None = None
    right: TimestampedValue[bool] | None = None
    center: TimestampedValue[bool] | None = None

    @classmethod
    def __struct_from_dict__(cls, d):
        # Convertir les valeurs en booléens avant la création de l'objet
        if d.get("left") and isinstance(d["left"].get("value"), str):
            d["left"]["value"] = d["left"]["value"].lower() == "true"
        if d.get("right") and isinstance(d["right"].get("value"), str):
            d["right"]["value"] = d["right"]["value"].lower() == "true"
        if d.get("center") and isinstance(d["center"].get("value"), str):
            d["center"]["value"] = d["center"]["value"].lower() == "true"
        return super().__struct_from_dict__(d)

    @staticmethod
    def _convert_to_bool(value):
        if isinstance(value, str):
            return value.lower() == "true"
        return bool(value)


class PassengerSeatbelt(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    front: TimestampedValue[bool] | None = None
    rear: RearPassengerSeatbelt | None = None

    @classmethod
    def __struct_from_dict__(cls, d):
        # Convertir la valeur front en booléen si nécessaire
        if d.get("front") and isinstance(d["front"].get("value"), str):
            d["front"]["value"] = d["front"]["value"].lower() == "true"
        return super().__struct_from_dict__(d)


class Seatbelt(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    driver: TimestampedValue[bool] | None = None
    passenger: PassengerSeatbelt | None = None

    @classmethod
    def __struct_from_dict__(cls, d):
        # Convertir la valeur driver en booléen si nécessaire
        if d.get("driver") and isinstance(d["driver"].get("value"), str):
            d["driver"]["value"] = d["driver"]["value"].lower() == "true"
        return super().__struct_from_dict__(d)


class MergedSeatbelt(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    driver: list[TimestampedValue[bool]] = []
    passenger_front: list[TimestampedValue[bool]] = []
    passenger_rear_left: list[TimestampedValue[bool]] = []
    passenger_rear_right: list[TimestampedValue[bool]] = []
    passenger_rear_center: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Seatbelt]) -> Self | None:
        if not lst:
            return None
        res = cls()

        # Gestion du conducteur
        res.driver = TimestampedValue.merge_list(
            [x for x in (e.driver for e in lst) if x is not None]
        )

        # Gestion des passagers avant
        res.passenger_front = TimestampedValue.merge_list(
            [
                x
                for x in (e.passenger.front if e.passenger else None for e in lst)
                if x is not None
            ]
        )

        # Gestion des passagers arrière
        res.passenger_rear_left = TimestampedValue.merge_list(
            [
                x
                for x in (
                    e.passenger.rear.left if e.passenger and e.passenger.rear else None
                    for e in lst
                )
                if x is not None
            ]
        )

        res.passenger_rear_right = TimestampedValue.merge_list(
            [
                x
                for x in (
                    e.passenger.rear.right if e.passenger and e.passenger.rear else None
                    for e in lst
                )
                if x is not None
            ]
        )

        res.passenger_rear_center = TimestampedValue.merge_list(
            [
                x
                for x in (
                    e.passenger.rear.center
                    if e.passenger and e.passenger.rear
                    else None
                    for e in lst
                )
                if x is not None
            ]
        )

        return res


class TirePairPressure(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    left: TimestampedValueWithUnit[float, PressureUnit] | None = None
    right: TimestampedValueWithUnit[float, PressureUnit] | None = None


class TirePressure(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: TirePairPressure | None = None
    rear: TirePairPressure | None = None


class MergedTirePressure(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    front_left: list[TimestampedValueWithUnit[float, PressureUnit]] = []
    front_right: list[TimestampedValueWithUnit[float, PressureUnit]] = []
    rear_left: list[TimestampedValueWithUnit[float, PressureUnit]] = []
    rear_right: list[TimestampedValueWithUnit[float, PressureUnit]] = []

    @classmethod
    def from_list(cls, lst: list[TirePressure]) -> Self:
        res = cls()
        res.front_left = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.front.left if e.front else None for e in lst)
                if x is not None
            ]
        )
        res.front_right = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.front.right if e.front else None for e in lst)
                if x is not None
            ]
        )
        res.rear_left = TimestampedValueWithUnit.merge_list(
            [x for x in (e.rear.left if e.rear else None for e in lst) if x is not None]
        )
        res.rear_right = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in (e.rear.right if e.rear else None for e in lst)
                if x is not None
            ]
        )
        return res


class TransmissionGearStateValue(StrEnum):
    n = "n"
    d1 = "d1"
    d2 = "d2"
    d3 = "d3"
    d4 = "d4"
    d5 = "d5"
    d6 = "d6"
    d7 = "d7"
    d8 = "d8"
    d9 = "d9"
    d2p = "d2p"
    r = "r"
    r2 = "r2"
    p = "p"
    disengaged = "disengaged"
    sna = "sna"


class TransmissionGear(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    state: TimestampedValue[TransmissionGearStateValue]


class PrivacyState(StrEnum):
    none = "none"
    geolocation = "geolocation"
    full = "full"


class SetupAdas(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    lane_keep_assist: TimestampedValue[bool] | None = None
    blind_spot_monitoring: TimestampedValue[bool] | None = None


class Setup(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    privacy: TimestampedValue[bool] | None = None
    privacy_state: TimestampedValue[PrivacyState] | None = None
    requested_privacy: TimestampedValue[bool] | None = None
    requested_privacy_state: TimestampedValue[PrivacyState] | None = None
    adas: SetupAdas | None = None


class MergedSetup(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    privacy: list[TimestampedValue[bool]] = []
    privacy_state: list[TimestampedValue[PrivacyState]] = []
    requested_privacy: list[TimestampedValue[bool]] = []
    requested_privacy_state: list[TimestampedValue[PrivacyState]] = []
    adas_lane_keep_assist: list[TimestampedValue[bool]] = []
    adas_blind_spot_monitoring: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Setup]) -> Self:
        res = cls()
        res.privacy = TimestampedValue.merge_list(
            [x for x in (e.privacy for e in lst) if x is not None]
        )
        res.privacy_state = TimestampedValue.merge_list(
            [x for x in (e.privacy_state for e in lst) if x is not None]
        )
        res.requested_privacy = TimestampedValue.merge_list(
            [x for x in (e.requested_privacy for e in lst) if x is not None]
        )
        res.requested_privacy_state = TimestampedValue.merge_list(
            [x for x in (e.requested_privacy_state for e in lst) if x is not None]
        )
        res.adas_lane_keep_assist = TimestampedValue.merge_list(
            [
                x
                for x in (e.adas.lane_keep_assist if e.adas else None for e in lst)
                if x is not None
            ]
        )
        res.adas_blind_spot_monitoring = TimestampedValue.merge_list(
            [
                x
                for x in (e.adas.blind_spot_monitoring if e.adas else None for e in lst)
                if x is not None
            ]
        )
        return res


class Maintenance(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    date: TimestampedValue[datetime.date] | None = None
    odometer: TimestampedValueWithUnit[float, DistanceUnit] | None = None


class Rear(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    need_repair: TimestampedValue[bool]


class MergedRear(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    need_repair: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Rear]) -> Self:
        res = cls()
        res.need_repair = TimestampedValue.merge_list(
            [x for x in (e.need_repair for e in lst) if x is not None]
        )
        return res


class Crash(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    auto_ecall: TimestampedValue[bool] | None = None
    pedestrian: TimestampedValue[bool] | None = None
    tipped_over: TimestampedValue[bool] | None = None
    rear: Rear | None = None


class MergedCrash(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    auto_ecall: list[TimestampedValue[bool]] = []
    pedestrian: list[TimestampedValue[bool]] = []
    tipped_over: list[TimestampedValue[bool]] = []
    rear: MergedRear | None = None

    @classmethod
    def from_list(cls, lst: list[Crash]) -> Self:
        res = cls()
        res.auto_ecall = TimestampedValue.merge_list(
            [x for x in (e.auto_ecall for e in lst) if x is not None]
        )
        res.pedestrian = TimestampedValue.merge_list(
            [x for x in (e.pedestrian for e in lst) if x is not None]
        )
        res.tipped_over = TimestampedValue.merge_list(
            [x for x in (e.tipped_over for e in lst) if x is not None]
        )
        res.rear = MergedRear.from_list(
            [x for x in (e.rear for e in lst) if x is not None]
        )
        return res


class CarState(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    _id: str | None = None
    vin: str | None = None
    datetime: dt | None = None
    datetimeSending: dt | None = None
    heading: (
        TimestampedValueWithUnit[
            Annotated[float, msgspec.Meta(ge=0, le=360)], AzimuthUnit
        ]
        | None
    ) = None
    geolocation: Geolocation | None = None
    odometer: TimestampedValueWithUnit[float, DistanceUnit] | None = None
    moving: TimestampedValue[bool] | None = None
    speed: TimestampedValueWithUnit[float, SpeedUnit] | None = None
    status: TimestampedValue[VehicleStatus] | None = None
    acceleration: TimestampedValueWithUnit[float, AccelerationUnit] | None = None
    acceleration_lat: TimestampedValueWithUnit[float, AccelerationUnit] | None = None
    fuel: Fuel | None = None
    electricity: Electricity | None = None
    engine: Engine | None = None
    external_temperature: TimestampedValueWithUnit[float, TemperatureUnit] | None = None
    adas: Adas | None = None
    alerts: TimestampedValue[list[str]] | None = None
    lights: Lights | None = None
    tire_pressure: TirePressure | None = None
    transmission_gear: TransmissionGear | None = None
    setup: Setup | None = None
    maintenance: Maintenance | None = None
    crash: Crash | None = None
    seatbelt: Seatbelt | None = None


class MergedCarState(
    msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"
):
    _id: str
    vin: str
    heading: list[
        TimestampedValueWithUnit[
            Annotated[float, msgspec.Meta(ge=0, le=360)], AzimuthUnit
        ]
    ] = []
    geolocation: list[Geolocation] = []
    odometer: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    moving: list[TimestampedValue[bool]] = []
    speed: list[TimestampedValueWithUnit[float, SpeedUnit]] = []
    status: list[TimestampedValue[VehicleStatus]] = []
    acceleration: list[TimestampedValueWithUnit[float, AccelerationUnit]] = []
    acceleration_lat: list[TimestampedValueWithUnit[float, AccelerationUnit]] = []
    fuel: MergedFuel | None = None
    electricity: MergedElectricity | None = None
    engine: MergedEngine | None = None
    external_temperature: list[TimestampedValueWithUnit[float, TemperatureUnit]] = []
    adas: MergedAdas | None = None
    alerts: list[TimestampedValue[list[str]]] = []
    lights: MergedLights | None = None
    tire_pressure: MergedTirePressure | None = None
    transmission_gear_state: list[TimestampedValue[TransmissionGearStateValue]] = []
    setup: MergedSetup | None = None
    maintenance_date: list[TimestampedValue[datetime.date]] = []
    maintenance_odometer: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    crash: MergedCrash | None = None
    seatbelt: MergedSeatbelt | None = None

    @classmethod
    def from_list(cls, lst: list[CarState]) -> Self | None:
        if len(lst) == 0:
            return None
        res = cls(
            _id=lst[0]._id,
            vin=lst[0].vin,
        )
        res.heading = TimestampedValueWithUnit.merge_list(
            [x for x in (e.heading for e in lst) if x is not None]
        )
        res.geolocation = Geolocation.merge_list(
            [x for x in (e.geolocation for e in lst) if x is not None]
        )
        res.odometer = TimestampedValueWithUnit.merge_list(
            [x for x in (e.odometer for e in lst) if x is not None]
        )
        res.moving = TimestampedValue.merge_list(
            [x for x in (e.moving for e in lst) if x is not None]
        )
        res.speed = TimestampedValueWithUnit.merge_list(
            [x for x in (e.speed for e in lst) if x is not None]
        )
        res.status = TimestampedValue.merge_list(
            [x for x in (e.status for e in lst) if x is not None]
        )
        res.acceleration = TimestampedValueWithUnit.merge_list(
            [x for x in (e.acceleration for e in lst) if x is not None]
        )
        res.acceleration_lat = TimestampedValueWithUnit.merge_list(
            [x for x in (e.acceleration_lat for e in lst) if x is not None]
        )
        res.fuel = MergedFuel.from_list(
            [x for x in (e.fuel for e in lst) if x is not None]
        )
        res.electricity = MergedElectricity.from_list(
            [x for x in (e.electricity for e in lst) if x is not None]
        )
        res.engine = MergedEngine.from_list(
            [x for x in (e.engine for e in lst) if x is not None]
        )
        res.external_temperature = TimestampedValueWithUnit.merge_list(
            [x for x in (e.external_temperature for e in lst) if x is not None]
        )
        res.adas = MergedAdas.from_list(
            [x for x in (e.adas for e in lst) if x is not None]
        )
        res.alerts = TimestampedValue.merge_list(
            [x for x in (e.alerts for e in lst) if x is not None]
        )
        res.lights = MergedLights.from_list(
            [x for x in (e.lights for e in lst) if x is not None]
        )
        res.tire_pressure = MergedTirePressure.from_list(
            [x for x in (e.tire_pressure for e in lst) if x is not None]
        )
        res.transmission_gear_state = TimestampedValue.merge_list(
            [
                x
                for x in (
                    e.transmission_gear.state if e.transmission_gear else None
                    for e in lst
                )
                if x is not None
            ]
        )
        res.setup = MergedSetup.from_list(
            [x for x in (e.setup for e in lst) if x is not None]
        )
        res.crash = MergedCrash.from_list(
            [x for x in (e.crash for e in lst) if x is not None]
        )
        res.seatbelt = MergedSeatbelt.from_list(
            [x for x in (e.seatbelt for e in lst) if x is not None]
        )
        return res


class ErrorMesage(msgspec.Struct):
    name: str
    message: str

