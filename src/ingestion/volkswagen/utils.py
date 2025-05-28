from datetime import UTC, datetime
from typing import TypeVar

T = TypeVar("T")


def ensure_exists(something: T | None, except_type=ValueError) -> T:
    if something is None:
        raise except_type("The given variable shan't be None")
    return something


def now(tz=UTC):
    return datetime.now(tz=tz)

