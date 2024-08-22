import datetime
from datetime import datetime as dt
from enum import StrEnum
from typing import Annotated, Generic, Optional, TypeVar

import msgspec

T = TypeVar("T")
U = TypeVar("U")


class WithTimestamp(
    msgspec.Struct,
    forbid_unknown_fields=True,
    omit_defaults=True,
    rename="camel",
):
    datetime: dt


class TimestampedValue(
    WithTimestamp,
    Generic[T],
):
    value: T


class ValueWithUnit(
    msgspec.Struct,
    Generic[T, U],
    forbid_unknown_fields=True,
    omit_defaults=True,
    rename="camel",
):
    value: T
    unit: U


class TimestampedValueWithUnit(
    WithTimestamp,
    Generic[T, U],
):
    value: T
    unit: U


class Percentage(
    msgspec.Struct,
):
    percentage: str | float

    def __post_init__(self):
        self.percentage = float(self.percentage)


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


class Geolocation(WithTimestamp):
    latitude: float
    longitude: float
    source: GpsSource
    gps_signal: Annotated[float, msgspec.Meta(ge=0, le=100)]
    altitude: TimestampedValueWithUnit[float, DistanceUnit]


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
    status: Optional[TimestampedValue[EngineStatus]] = None
    speed: Optional[TimestampedValueWithUnit[float, EngineSpeedUnit]] = None


class Fuel(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    average_consumption: Optional[
        TimestampedValueWithUnit[float, FuelConsumptionUnit]
    ] = None
    instant_consumption: Optional[
        TimestampedValueWithUnit[float, FuelConsumptionUnit]
    ] = None
    total_consumption: Optional[TimestampedValueWithUnit[float, VolumeUnit]] = None
    level: Optional[FuelConsumptionLevel] = None
    residual_autonomy: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None
    engine: Optional[EngineSmall] = None


class EnergyConsumptionUnit(StrEnum):
    kilowatthours_per_100_kilometers = "kWh/100 km"


class EnergyUnit(StrEnum):
    killowatt_hours = "kWh"


class EnergyConsumptionLevel(WithTimestamp):
    value: float
    unit: EnergyUnit
    percentage: str | float

    def __post_init__(self):
        self.percentage = float(self.percentage)


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


class Charging(WithTimestamp):
    plugged: bool
    status: ChargingStatus
    remaining_time: Duration
    rate: ValueWithUnit[float, ChargingRateUnit]
    mode: ChargingMode
    planned: dt


class Electricity(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    instant_consumption: Optional[
        TimestampedValueWithUnit[float, EnergyConsumptionUnit]
    ] = None
    level: Optional[EnergyConsumptionLevel] = None
    residual_autonomy: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None
    capacity: Optional[TimestampedValueWithUnit[float, CapacityUnit]] = None
    charging: Optional[Charging] = None
    engine: Optional[EngineSmall] = None


class TemperatureUnit(StrEnum):
    celcius_degrees = "°C"


class PressureUnit(StrEnum):
    bar = "bar"
    psi = "psi"


class EngineOil(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    temperature: Optional[TimestampedValueWithUnit[float, TemperatureUnit]] = None
    pressure: Optional[TimestampedValueWithUnit[float, PressureUnit]] = None
    life_left: Optional[Percentage] = None


class VoltageUnit(StrEnum):
    volts = "V"


class EngineBattery(WithTimestamp):
    capacity: Optional[Percentage] = None
    resistance: Optional[object] = None
    voltage: Optional[TimestampedValueWithUnit[float, VoltageUnit]] = None


class EngineCoolant(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    temperature: TimestampedValueWithUnit[float, TemperatureUnit]


class Engine(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    oil: Optional[EngineOil] = None
    contact: Optional[TimestampedValue[bool]] = None
    status: Optional[TimestampedValue[EngineStatus]] = None
    speed: Optional[TimestampedValueWithUnit[float, EngineSpeedUnit]] = None
    ignition: Optional[TimestampedValue[bool]] = None
    battery: Optional[EngineBattery] = None
    run_time: Optional[TimestampedValue[float]] = None
    coolant: Optional[EngineCoolant] = None


class ParkAssistValue(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    alarm: TimestampedValue[bool]
    muted: TimestampedValue[bool]


class ParkAssist(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: ParkAssistValue
    rear: ParkAssistValue


class KeepAssist(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    right: TimestampedValue[bool]
    left: TimestampedValue[bool]


class Lane(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    keep_assist: KeepAssist


class Sli(WithTimestamp):
    value: float
    unit: SpeedUnit


class Adas(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    park_assist: Optional[ParkAssist] = None
    lane: Optional[Lane] = None
    esp: Optional[TimestampedValue[bool]] = None
    abs: Optional[TimestampedValue[bool]] = None
    blind_spot_monitoring: Optional[TimestampedValue[bool]] = None
    fse: Optional[TimestampedValue[bool]] = None
    sli: Optional[Sli] = None


class FogLights(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: TimestampedValue[bool]
    rear: TimestampedValue[bool]


class Turn(StrEnum):
    left = "left"
    right = "right"


class Lights(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    fog: Optional[FogLights] = None
    turn: Optional[TimestampedValue[Turn]] = None
    warnings: Optional[TimestampedValue[bool]] = None


class RearPassengerSeatbelt(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    left: TimestampedValue[bool]
    right: TimestampedValue[bool]
    central: TimestampedValue[bool]


class PassengerSeatbelt(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: TimestampedValue[bool]
    rear: RearPassengerSeatbelt


class Seatbelt(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    driver: Optional[TimestampedValue[bool]] = None
    passenger: Optional[PassengerSeatbelt] = None


class TirePairPressure(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    left: TimestampedValueWithUnit[float, PressureUnit]
    right: TimestampedValueWithUnit[float, PressureUnit]


class TirePressure(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    front: Optional[TirePairPressure] = None
    rear: Optional[TirePairPressure] = None


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
    lane_keep_assist: TimestampedValue[bool]
    blind_spot_monitoring: TimestampedValue[bool]


class Setup(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    privacy: Optional[TimestampedValue[bool]] = None
    privacy_state: Optional[TimestampedValue[PrivacyState]] = None
    requested_privacy: Optional[TimestampedValue[bool]] = None
    requested_privacy_state: Optional[TimestampedValue[PrivacyState]] = None
    adas: Optional[SetupAdas] = None


class Maintenance(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    date: Optional[TimestampedValue[datetime.date]] = None
    odometer: Optional[TimestampedValueWithUnit[float, DistanceUnit]] = None


class Crash(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    auto_ecall: Optional[TimestampedValue[bool]] = None
    pedestrian: Optional[TimestampedValue[bool]] = None
    tipped_over: Optional[TimestampedValue[bool]] = None


class CarState(
    msgspec.Struct, forbid_unknown_fields=True, omit_defaults=True, rename="camel"
):
    _id: Annotated[str, msgspec.Meta(pattern="^[a-z0-9]{24}$")]
    vin: Annotated[str, msgspec.Meta(pattern="^[A-Z0-9]{17}$")]
    datetime: dt
    datetime_sending: dt
    heading: Optional[
        TimestampedValueWithUnit[
            Annotated[float, msgspec.Meta(ge=0, le=360)], AzimuthUnit
        ]
    ] = None
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
    external_temperature: Optional[TimestampedValueWithUnit[float, TemperatureUnit]] = (
        None
    )
    adas: Optional[Adas] = None
    alerts: Optional[TimestampedValue[list[str]]] = None
    lights: Optional[Lights] = None
    seatbelt: Optional[Seatbelt] = None
    tire_pressure: Optional[TirePressure] = None
    transmission_gear: Optional[TransmissionGear] = None
    setup: Optional[Setup] = None
    maintenance: Optional[Maintenance] = None
    crash: Optional[Crash] = None


class ErrorMesage(msgspec.Struct):
    name: str
    message: str

