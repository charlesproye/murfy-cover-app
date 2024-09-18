from threading import Lock
from typing import Generic, Type, TypeVar

from ingestion.high_mobility.schema import (
    MergedInfoProtocol,
)

U = TypeVar("U")
V = TypeVar("V", bound=MergedInfoProtocol)


class MergedInfoWrapper(Generic[U, V]):
    info: V
    __lock: Lock

    def __init__(self, typ: Type[V]) -> None:
        self.info = typ.new()
        self.__lock = Lock()

    def set_info(self, info: U) -> None:
        with self.__lock:
            self.info = type(self.info).from_initial(info)

    def merge(self, other: U):
        with self.__lock:
            if self.info:
                self.info.merge(other)

