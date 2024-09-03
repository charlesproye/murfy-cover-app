from threading import Lock
from typing import Optional

from ingestion.high_mobility.schema import (
    KiaInfo,
    MercedesBenzInfo,
    MergedKiaInfo,
    MergedMercedesBenzInfo,
    MergedRenaultInfo,
    RenaultInfo,
)


class MergedInfoWrapper:
    info: Optional[MergedKiaInfo | MergedRenaultInfo | MergedMercedesBenzInfo]
    __lock: Lock

    def __init__(self) -> None:
        self.info = None
        self.__lock = Lock()

    def set_info(self, info: KiaInfo | RenaultInfo | MercedesBenzInfo) -> None:
        with self.__lock:
            match info:
                case KiaInfo():
                    self.info = MergedKiaInfo.from_initial(info)
                case RenaultInfo():
                    self.info = MergedRenaultInfo.from_initial(info)
                case MercedesBenzInfo():
                    self.info = MergedMercedesBenzInfo.from_initial(info)
                case _:
                    return

    def merge(self, other):
        with self.__lock:
            self.info.merge(other)

