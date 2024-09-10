from datetime import datetime
from typing import Generic, Optional, Protocol, Self, TypeVar

import msgspec
from ingestion.high_mobility.schema.factory import Brand, make_all_brands

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


T = TypeVar("T", contravariant=True)


class MergedInfoProtocol(Protocol[T]):
    @classmethod
    def new(cls) -> Self: ...
    @classmethod
    def from_initial(cls, initial: T) -> Self: ...
    def merge(self, other: T) -> None: ...


all_brands: dict[str, Brand] = make_all_brands()

