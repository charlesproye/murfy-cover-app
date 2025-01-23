import datetime
from datetime import datetime as dt
from enum import StrEnum
from typing import Annotated, Optional, Self, TypeVar, Generic, Iterable

import msgspec

T = TypeVar("T")
U = TypeVar("U")


class WithTimestamp(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    datetime: dt


class TimestampedValue(WithTimestamp, Generic[T]):
    value: T

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in set(map(lambda e: e.datetime, res)):
                res.append(el)
        return res


class ValueWithUnit(msgspec.Struct, Generic[T, U], forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    value: T
    unit: U


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
            elif el is not None:
                if el.datetime not in set(map(lambda e: e.datetime, res)):
                    res.append(el)
        return res


class Percentage(WithTimestamp, forbid_unknown_fields=False):
    percentage: str | float

    def __post_init__(self):
        self.percentage = float(self.percentage)

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in set(map(lambda e: e.datetime, res)):
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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    source: Optional[GpsSource] = None
    gps_signal: Optional[Annotated[float, msgspec.Meta(ge=0, le=100)]] = None
    altitude: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
                
        for el in lst:
            if isinstance(el, list):
                # Si c'est une liste, on l'aplatit
                for item in el:
                    if item and hasattr(item, 'datetime'):
                        if item.datetime not in set(map(lambda e: e.datetime, res)):
                            res.append(item)
            elif el and hasattr(el, 'datetime'):
                if el.datetime not in set(map(lambda e: e.datetime, res)):
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
    unit: Optional[VolumeUnit] = None
    value: Optional[float] = None

    def __post_init__(self):
        self.percentage = float(self.percentage)

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in set(map(lambda e: e.datetime, res)):
                res.append(el)
        return res


class EngineStatus(StrEnum):
    off = "off"
    starting = "starting"
    running = "running"
    start_and_stop = "start-and-stop"


class EngineSpeedUnit(StrEnum):
    revolutions_per_minute = "rpm"


class EngineSmall(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    status: Optional[TimestampedValue[EngineStatus]] = None
    speed: Optional[TimestampedValueWithUnit[float, EngineSpeedUnit]] = None


class Fuel(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    average_consumption: Optional[TimestampedValueWithUnit[float, FuelConsumptionUnit]] = None
    instant_consumption: Optional[TimestampedValueWithUnit[float, FuelConsumptionUnit]] = None
    total_consumption: Optional[TimestampedValueWithUnit[float, VolumeUnit]] = None
    level: Optional[FuelConsumptionLevel] = None
    residual_autonomy: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None
    engine: Optional[EngineSmall] = None


class MergedFuel(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
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
            [x for x in map(lambda e: e.average_consumption, lst) if x is not None]
        )
        res.instant_consumption = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.instant_consumption, lst) if x is not None]
        )
        res.total_consumption = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.total_consumption, lst) if x is not None]
        )
        res.level = FuelConsumptionLevel.merge_list(
            [x for x in map(lambda e: e.level, lst) if x is not None]
        )
        res.residual_autonomy = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.residual_autonomy, lst) if x is not None]
        )
        res.engine_speed = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(lambda e: e.engine.speed if e.engine else None, lst)
                if x is not None
            ]
        )
        return res


class EnergyConsumptionUnit(StrEnum):
    kilowatthours_per_100_kilometers = "kWh/100 km"


class EnergyUnit(StrEnum):
    killowatt_hours = "kWh"


class EnergyConsumptionLevel(WithTimestamp):
    value: Optional[float] = None  # Rendre 'value' optionnel
    unit: Optional[EnergyUnit] = None  # Rendre 'unit' optionnel
    percentage: Optional[str | float] = None  # Rendre 'percentage' optionnel

    def __post_init__(self):
        if self.percentage is not None:
            self.percentage = float(self.percentage)

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in set(map(lambda e: e.datetime, res)):
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
    plugged: Optional[bool] = None
    status: Optional[ChargingStatus] = None
    remainingTime: Optional[int] = None
    mode: Optional[ChargingMode] = None
    planned: Optional[dt] = None
    rate: Optional[int | ValueWithUnit[float, ChargingRateUnit]] = None

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in set(map(lambda e: e.datetime, res)):
                res.append(el)
        return res


class Battery(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    stateOfHealth: Optional[Percentage] = None


class MergedBattery(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    stateOfHealth: list[Percentage] = []

    @classmethod
    def from_list(cls, lst: list[Battery]) -> Optional[Self]:
        if not lst:
            return None
        res = cls()
        res.stateOfHealth = [x for x in map(lambda e: e.stateOfHealth, lst) if x is not None]
        return res


class Electricity(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    capacity: Optional[TimestampedValueWithUnit[float, CapacityUnit]] = None
    charging: Optional[Charging] = None
    engine: Optional[EngineSmall] = None
    residual_autonomy: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None
    level: Optional[EnergyConsumptionLevel] = None
    instant_consumption: Optional[TimestampedValueWithUnit[float, EnergyConsumptionUnit]] = None
    battery: Optional[Battery] = None

    @classmethod
    def __struct_from_dict__(cls, d):
        if 'capacity' in d:
            d['battery_capacity'] = d.pop('capacity')
        return super().__struct_from_dict__(d)


class MergedElectricity(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    instant_consumption: list[TimestampedValueWithUnit[float, EnergyConsumptionUnit]] = []
    level: list[EnergyConsumptionLevel] = []
    residual_autonomy: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    battery_capacity: list[TimestampedValueWithUnit[float, CapacityUnit]] = []
    charging: list[Charging] = []
    engine_speed: list[TimestampedValueWithUnit[float, EngineSpeedUnit]] = []
    battery: Optional[MergedBattery] = None

    @classmethod
    def from_list(cls, lst: list[Electricity]) -> Self:
        res = cls()
        res.instant_consumption = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: getattr(e, 'instant_consumption', None), lst) if x is not None]
        )
        res.level = EnergyConsumptionLevel.merge_list(
            [x for x in map(lambda e: e.level, lst) if x is not None]
        )
        res.residual_autonomy = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.residual_autonomy, lst) if x is not None]
        )
        res.battery_capacity = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.capacity, lst) if x is not None]
        )
        res.charging = Charging.merge_list(
            [x for x in map(lambda e: e.charging, lst) if x is not None]
        )
        res.engine_speed = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(lambda e: e.engine.speed if e.engine else None, lst)
                if x is not None
            ]
        )
        res.battery = MergedBattery.from_list(
            [x for x in map(lambda e: e.battery, lst) if x is not None]
        )
        return res


class TemperatureUnit(StrEnum):
    celcius_degrees = "°C"


class PressureUnit(StrEnum):
    bar = "bar"
    psi = "psi"


class EngineOil(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    temperature: Optional[TimestampedValueWithUnit[float, TemperatureUnit]] = None
    pressure: Optional[TimestampedValueWithUnit[float, PressureUnit]] = None
    life_left: Optional[Percentage] = None


class VoltageUnit(StrEnum):
    volts = "V"


class EngineBattery(WithTimestamp):
    capacity: Optional[Percentage] = None
    resistance: Optional[object] = None
    voltage: Optional[TimestampedValueWithUnit[float, VoltageUnit]] = None

    @classmethod
    def merge_list(cls, lst: Iterable[Self]) -> list[Self]:
        res: list[Self] = []
        for el in lst:
            if el.datetime not in set(map(lambda e: e.datetime, res)):
                res.append(el)
        return res


class EngineCoolant(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    temperature: TimestampedValueWithUnit[float, TemperatureUnit]


class Engine(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    oil: Optional[EngineOil] = None
    contact: Optional[TimestampedValue[bool]] = None
    status: Optional[TimestampedValue[EngineStatus]] = None
    speed: Optional[TimestampedValueWithUnit[float, EngineSpeedUnit]] = None
    ignition: Optional[TimestampedValue[bool]] = None
    battery: Optional[EngineBattery] = None
    percentage: Optional[Percentage] = None
    run_time: Optional[TimestampedValue[float]] = None
    coolant: Optional[EngineCoolant] = None


class MergedEngine(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
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
                for x in map(lambda e: e.oil.temperature if e.oil else None, lst)
                if x is not None
            ]
        )
        res.oil_pressure = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(lambda e: e.oil.pressure if e.oil else None, lst)
                if x is not None
            ]
        )
        res.contact = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.contact, lst) if x is not None]
        )
        res.status = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.status, lst) if x is not None]
        )
        res.speed = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.speed, lst) if x is not None]
        )
        res.ignition = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.ignition, lst) if x is not None]
        )
        res.battery = EngineBattery.merge_list(
            [x for x in map(lambda e: e.battery, lst) if x is not None]
        )
        res.run_time = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.run_time, lst) if x is not None]
        )
        res.coolant_temperature = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(
                    lambda e: e.coolant.temperature if e.coolant else None, lst
                )
                if x is not None
            ]
        )
        return res


class ParkAssistValue(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    alarm: Optional[TimestampedValue[bool]] = None
    muted: Optional[TimestampedValue[bool]] = None


class MergedParkAssistValue(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    alarm: list[TimestampedValue[bool]] = []
    muted: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[ParkAssistValue]) -> Self:
        res = cls()
        res.alarm = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.alarm, lst) if x is not None]
        )
        res.muted = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.muted, lst) if x is not None]
        )
        return res


class ParkAssist(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    front: ParkAssistValue
    rear: ParkAssistValue


class MergedParkAssist(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    front: Optional[MergedParkAssistValue] = None
    rear: Optional[MergedParkAssistValue] = None

    @classmethod
    def from_list(cls, lst: list[ParkAssist]) -> Self:
        res = cls()
        res.front = MergedParkAssistValue.from_list(
            [x for x in map(lambda e: e.front, lst) if x is not None]
        )
        res.rear = MergedParkAssistValue.from_list(
            [x for x in map(lambda e: e.rear, lst) if x is not None]
        )
        return res


class KeepAssist(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    right: Optional[TimestampedValue[bool]] = None
    left: Optional[TimestampedValue[bool]] = None


class Lane(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    keep_assist: Optional[KeepAssist] = None


class MergedLaneKeepAssist(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    right: list[TimestampedValue[bool]] = []
    left: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Lane]) -> Self:
        res = cls()
        res.left = TimestampedValue.merge_list(
            [
                x
                for x in map(
                    lambda e: e.keep_assist.left if e.keep_assist else None, lst
                )
                if x is not None
            ]
        )
        res.right = TimestampedValue.merge_list(
            [
                x
                for x in map(
                    lambda e: e.keep_assist.right if e.keep_assist else None, lst
                )
                if x is not None
            ]
        )
        return res


class Adas(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    park_assist: Optional[ParkAssist] = None
    lane: Optional[Lane] = None
    esp: Optional[TimestampedValue[bool]] = None
    abs: Optional[TimestampedValue[bool]] = None
    blind_spot_monitoring: Optional[TimestampedValue[bool]] = None
    fse: Optional[TimestampedValue[bool]] = None
    sli: Optional[TimestampedValueWithUnit[float, SpeedUnit]] = None


class MergedAdas(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    park_assist: Optional[MergedParkAssist] = None
    lane_keep_assist: Optional[MergedLaneKeepAssist] = None
    esp: list[TimestampedValue[bool]] = []
    abs: list[TimestampedValue[bool]] = []
    blind_spot_monitoring: list[TimestampedValue[bool]] = []
    fse: list[TimestampedValue[bool]] = []
    sli: list[TimestampedValueWithUnit[float, SpeedUnit]] = []

    @classmethod
    def from_list(cls, lst: list[Adas]) -> Self:
        res = cls()
        res.park_assist = MergedParkAssist.from_list(
            [x for x in map(lambda e: e.park_assist, lst) if x is not None]
        )
        res.lane_keep_assist = MergedLaneKeepAssist.from_list(
            [x for x in map(lambda e: e.lane, lst) if x is not None]
        )
        res.esp = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.esp, lst) if x is not None]
        )
        res.abs = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.abs, lst) if x is not None]
        )
        res.blind_spot_monitoring = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.blind_spot_monitoring, lst) if x is not None]
        )
        res.fse = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.fse, lst) if x is not None]
        )
        res.sli = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.sli, lst) if x is not None]
        )
        return res


class FogLights(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    front: TimestampedValue[bool]
    rear: TimestampedValue[bool]


class Turn(StrEnum):
    left = "left"
    right = "right"


class Lights(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    fog: Optional[FogLights] = None
    turn: Optional[TimestampedValue[Turn]] = None
    warnings: Optional[TimestampedValue[bool]] = None


class MergedLights(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    fog_front: list[TimestampedValue[bool]] = []
    fog_rear: list[TimestampedValue[bool]] = []
    turn: list[TimestampedValue[Turn]] = []
    warnings: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Lights]) -> Self:
        res = cls()
        res.fog_front = TimestampedValue.merge_list(
            [
                x
                for x in map(lambda e: e.fog.front if e.fog else None, lst)
                if x is not None
            ]
        )
        res.fog_rear = TimestampedValue.merge_list(
            [
                x
                for x in map(lambda e: e.fog.rear if e.fog else None, lst)
                if x is not None
            ]
        )
        res.turn = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.turn, lst) if x is not None]
        )
        res.warnings = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.warnings, lst) if x is not None]
        )
        return res


class RearPassengerSeatbelt(msgspec.Struct, forbid_unknown_fields=False):
    left: Optional[TimestampedValue[bool]] = None
    right: Optional[TimestampedValue[bool]] = None
    center: Optional[TimestampedValue[bool]] = None

    @classmethod
    def __struct_from_dict__(cls, d):
        # Convertir les valeurs en booléens avant la création de l'objet
        if d.get('left') and isinstance(d['left'].get('value'), str):
            d['left']['value'] = d['left']['value'].lower() == 'true'
        if d.get('right') and isinstance(d['right'].get('value'), str):
            d['right']['value'] = d['right']['value'].lower() == 'true'
        if d.get('center') and isinstance(d['center'].get('value'), str):
            d['center']['value'] = d['center']['value'].lower() == 'true'
        return super().__struct_from_dict__(d)

    @staticmethod
    def _convert_to_bool(value):
        if isinstance(value, str):
            return value.lower() == 'true'
        return bool(value)


class PassengerSeatbelt(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    front: Optional[TimestampedValue[bool]] = None
    rear: Optional[RearPassengerSeatbelt] = None

    @classmethod
    def __struct_from_dict__(cls, d):
        # Convertir la valeur front en booléen si nécessaire
        if d.get('front') and isinstance(d['front'].get('value'), str):
            d['front']['value'] = d['front']['value'].lower() == 'true'
        return super().__struct_from_dict__(d)

class Seatbelt(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    driver: Optional[TimestampedValue[bool]] = None
    passenger: Optional[PassengerSeatbelt] = None

    @classmethod
    def __struct_from_dict__(cls, d):
        # Convertir la valeur driver en booléen si nécessaire
        if d.get('driver') and isinstance(d['driver'].get('value'), str):
            d['driver']['value'] = d['driver']['value'].lower() == 'true'
        return super().__struct_from_dict__(d)

class MergedSeatbelt(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    driver: list[TimestampedValue[bool]] = []
    passenger_front: list[TimestampedValue[bool]] = []
    passenger_rear_left: list[TimestampedValue[bool]] = []
    passenger_rear_right: list[TimestampedValue[bool]] = []
    passenger_rear_center: list[TimestampedValue[bool]] = []

    @classmethod
    def from_list(cls, lst: list[Seatbelt]) -> Optional[Self]:
        if not lst:
            return None
        res = cls()
        
        # Gestion du conducteur
        res.driver = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.driver, lst) if x is not None]
        )
        
        # Gestion des passagers avant
        res.passenger_front = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.passenger.front if e.passenger else None, lst) if x is not None]
        )
        
        # Gestion des passagers arrière
        res.passenger_rear_left = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.passenger.rear.left if e.passenger and e.passenger.rear else None, lst) if x is not None]
        )
        
        res.passenger_rear_right = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.passenger.rear.right if e.passenger and e.passenger.rear else None, lst) if x is not None]
        )
        
        res.passenger_rear_center = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.passenger.rear.center if e.passenger and e.passenger.rear else None, lst) if x is not None]
        )
        
        return res


class TirePairPressure(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    left: Optional[TimestampedValueWithUnit[float, PressureUnit]] = None
    right: Optional[TimestampedValueWithUnit[float, PressureUnit]] = None


class TirePressure(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    front: Optional[TirePairPressure] = None
    rear: Optional[TirePairPressure] = None



class MergedTirePressure(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
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
                for x in map(lambda e: e.front.left if e.front else None, lst)
                if x is not None
            ]
        )
        res.front_right = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(lambda e: e.front.right if e.front else None, lst)
                if x is not None
            ]
        )
        res.rear_left = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(lambda e: e.rear.left if e.rear else None, lst)
                if x is not None
            ]
        )
        res.rear_right = TimestampedValueWithUnit.merge_list(
            [
                x
                for x in map(lambda e: e.rear.right if e.rear else None, lst)
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


class TransmissionGear(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    state: TimestampedValue[TransmissionGearStateValue]


class PrivacyState(StrEnum):
    none = "none"
    geolocation = "geolocation"
    full = "full"


class SetupAdas(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    lane_keep_assist: TimestampedValue[bool]
    blind_spot_monitoring: TimestampedValue[bool]


class Setup(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    privacy: Optional[TimestampedValue[bool]] = None
    privacy_state: Optional[TimestampedValue[PrivacyState]] = None
    requested_privacy: Optional[TimestampedValue[bool]] = None
    requested_privacy_state: Optional[TimestampedValue[PrivacyState]] = None
    adas: Optional[SetupAdas] = None


class MergedSetup(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
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
            [x for x in map(lambda e: e.privacy, lst) if x is not None]
        )
        res.privacy_state = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.privacy_state, lst) if x is not None]
        )
        res.requested_privacy = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.requested_privacy, lst) if x is not None]
        )
        res.requested_privacy_state = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.requested_privacy_state, lst) if x is not None]
        )
        res.adas_lane_keep_assist = TimestampedValue.merge_list(
            [
                x
                for x in map(lambda e: e.adas.lane_keep_assist if e.adas else None, lst)
                if x is not None
            ]
        )
        res.adas_blind_spot_monitoring = TimestampedValue.merge_list(
            [
                x
                for x in map(
                    lambda e: e.adas.blind_spot_monitoring if e.adas else None, lst
                )
                if x is not None
            ]
        )
        return res


class Maintenance(msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"):
    date: Optional[TimestampedValue[datetime.date]] = None
    odometer: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None


class Crash(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    auto_ecall: Optional[TimestampedValue[bool]] = None
    pedestrian: Optional[TimestampedValue[bool]] = None
    tipped_over: Optional[TimestampedValue[bool]] = None
    rear: Optional[TimestampedValue[dict]] = None  # Changed to dict to accept any fields


class MergedCrash(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    auto_ecall: list[TimestampedValue[bool]] = []
    pedestrian: list[TimestampedValue[bool]] = []
    tipped_over: list[TimestampedValue[bool]] = []
    rear: list[TimestampedValue[bool | dict]] = []

    @classmethod
    def from_list(cls, lst: list[Crash]) -> Self:
        res = cls()
        res.auto_ecall = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.auto_ecall, lst) if x is not None]
        )
        res.pedestrian = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.pedestrian, lst) if x is not None]
        )
        res.tipped_over = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.tipped_over, lst) if x is not None]
        )
        res.rear = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.rear, lst) if x is not None]
        )
        return res


class CarState(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    _id: Optional[str] = None
    vin: Optional[str] = None
    datetime: Optional[dt] = None
    datetimeSending: Optional[dt] = None
    heading: Optional[TimestampedValueWithUnit[Annotated[float, msgspec.Meta(ge=0, le=360)], AzimuthUnit]] = None
    geolocation: Optional[Geolocation] = None
    odometer: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None
    moving: Optional[TimestampedValue[bool]] = None
    speed: Optional[TimestampedValueWithUnit[float, SpeedUnit]] = None
    status: Optional[TimestampedValue[VehicleStatus]] = None
    acceleration: Optional[TimestampedValueWithUnit[float, AccelerationUnit]] = None
    acceleration_lat: Optional[TimestampedValueWithUnit[float, AccelerationUnit]] = None
    fuel: Optional[Fuel] = None
    electricity: Optional[Electricity] = None
    engine: Optional[Engine] = None
    external_temperature: Optional[TimestampedValueWithUnit[float, TemperatureUnit]] = None
    adas: Optional[Adas] = None
    alerts: Optional[TimestampedValue[list[str]]] = None
    lights: Optional[Lights] = None
    tire_pressure: Optional[TirePressure] = None
    transmission_gear: Optional[TransmissionGear] = None
    setup: Optional[Setup] = None
    maintenance: Optional[Maintenance] = None
    crash: Optional[Crash] = None


class MergedCarState(msgspec.Struct, forbid_unknown_fields=False, omit_defaults=True, rename="camel"):
    _id: str
    vin: str
    heading: list[TimestampedValueWithUnit[Annotated[float, msgspec.Meta(ge=0, le=360)], AzimuthUnit]] = []
    geolocation: list[Geolocation] = []
    odometer: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    moving: list[TimestampedValue[bool]] = []
    speed: list[TimestampedValueWithUnit[float, SpeedUnit]] = []
    status: list[TimestampedValue[VehicleStatus]] = []
    acceleration: list[TimestampedValueWithUnit[float, AccelerationUnit]] = []
    acceleration_lat: list[TimestampedValueWithUnit[float, AccelerationUnit]] = []
    fuel: Optional[MergedFuel] = None
    electricity: Optional[MergedElectricity] = None
    engine: Optional[MergedEngine] = None
    external_temperature: list[TimestampedValueWithUnit[float, TemperatureUnit]] = []
    adas: Optional[MergedAdas] = None
    alerts: list[TimestampedValue[list[str]]] = []
    lights: Optional[MergedLights] = None
    tire_pressure: Optional[MergedTirePressure] = None
    transmission_gear_state: list[TimestampedValue[TransmissionGearStateValue]] = []
    setup: Optional[MergedSetup] = None
    maintenance_date: list[TimestampedValue[datetime.date]] = []
    maintenance_odometer: list[TimestampedValueWithUnit[float, DistanceUnit]] = []
    crash: Optional[MergedCrash] = None
    seatbelt: Optional[MergedSeatbelt] = None  # Ajout du champ seatbelt

    @classmethod
    def from_list(cls, lst: list[CarState]) -> Optional[Self]:
        if len(lst) == 0:
            return None
        res = cls(
            _id=lst[0]._id,
            vin=lst[0].vin,
        )
        res.heading = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.heading, lst) if x is not None]
        )
        res.geolocation = Geolocation.merge_list(
            [x for x in map(lambda e: e.geolocation, lst) if x is not None]
        )
        res.odometer = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.odometer, lst) if x is not None]
        )
        res.moving = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.moving, lst) if x is not None]
        )
        res.speed = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.speed, lst) if x is not None]
        )
        res.status = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.status, lst) if x is not None]
        )
        res.acceleration = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.acceleration, lst) if x is not None]
        )
        res.acceleration_lat = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.acceleration_lat, lst) if x is not None]
        )
        res.fuel = MergedFuel.from_list(
            [x for x in map(lambda e: e.fuel, lst) if x is not None]
        )
        res.electricity = MergedElectricity.from_list(
            [x for x in map(lambda e: e.electricity, lst) if x is not None]
        )
        res.engine = MergedEngine.from_list(
            [x for x in map(lambda e: e.engine, lst) if x is not None]
        )
        res.external_temperature = TimestampedValueWithUnit.merge_list(
            [x for x in map(lambda e: e.external_temperature, lst) if x is not None]
        )
        res.adas = MergedAdas.from_list(
            [x for x in map(lambda e: e.adas, lst) if x is not None]
        )
        res.alerts = TimestampedValue.merge_list(
            [x for x in map(lambda e: e.alerts, lst) if x is not None]
        )
        res.lights = MergedLights.from_list(
            [x for x in map(lambda e: e.lights, lst) if x is not None]
        )
        res.tire_pressure = MergedTirePressure.from_list(
            [x for x in map(lambda e: e.tire_pressure, lst) if x is not None]
        )
        res.transmission_gear_state = TimestampedValue.merge_list(
            [
                x
                for x in map(
                    lambda e: e.transmission_gear.state
                    if e.transmission_gear
                    else None,
                    lst,
                )
                if x is not None
            ]
        )
        res.setup = MergedSetup.from_list(
            [x for x in map(lambda e: e.setup, lst) if x is not None]
        )
        res.crash = MergedCrash.from_list(
            [x for x in map(lambda e: e.crash, lst) if x is not None]
        )
        res.seatbelt = MergedSeatbelt.from_list(
            [x for x in map(lambda e: e.seatbelt, lst) if x is not None]
        )
        return res


class ErrorMesage(msgspec.Struct):
    name: str
    message: str
