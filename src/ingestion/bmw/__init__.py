from datetime import datetime
from typing import Generic, Optional, Protocol, Self, TypeVar

import msgspec
T = TypeVar("T")


class Failure(msgspec.Struct):
    description: str
    reason: str


class BMWApiValue(msgspec.Struct, Generic[T]):
    timestamp: datetime
    failure: Optional[Failure]
    data: T


def is_new_value(lst: list[BMWApiValue[T]], new: Optional[BMWApiValue[T]]) -> bool:
    if new is None:
        return False
    else:
        return new.timestamp not in set(map(lambda o: o.timestamp, lst))


class BMWApiResponse(msgspec.Struct):
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


T = TypeVar("T", contravariant=True)


class MergedInfoProtocol(Protocol[T]):
    @classmethod
    def new(cls) -> Self: ...
    @classmethod
    def from_initial(cls, initial: T) -> Self: ...
    def merge(self, other: T) -> None: ...

