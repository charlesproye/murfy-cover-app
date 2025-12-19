from datetime import UTC, datetime
from typing import TypeVar

T = TypeVar("T")


def ensure_exists(something: T | None, except_type=ValueError) -> T:
    if something is None:
        raise except_type("The given variable shan't be None")
    return something


def remove_none(listing: list[T | None]) -> list[T]:
    return [elem for elem in listing if elem is not None]


def now(tz=UTC):
    return datetime.now(tz=tz)
