from threading import Lock
from typing import Generic, Type, TypeVar

from ingestion.high_mobility.schema import (
    BmwInfo,
    KiaInfo,
    MercedesBenzInfo,
    RenaultInfo,
)
from ingestion.high_mobility.schema.merged import (
    MergedInfoProtocol,
)

U = TypeVar("U", BmwInfo, KiaInfo, MercedesBenzInfo, RenaultInfo)
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
            # match info:
            #     case KiaInfo():
            #         self.info = MergedKiaInfo.from_initial(info)
            #     case RenaultInfo():
            #         self.info = MergedRenaultInfo.from_initial(info)
            #     case MercedesBenzInfo():
            #         self.info = MergedMercedesBenzInfo.from_initial(info)
            #     case _:
            #         return

    def merge(self, other: U):
        with self.__lock:
            if self.info:
                self.info.merge(other)

