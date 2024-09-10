from typing import Optional, Self, cast

import msgspec

from .. import (
    DataWithUnit,
    HMApiResponse,
    HMApiValue,
    is_new_value,
)
from ..factory import register_brand, register_merged


class KiaDiagnostics(msgspec.Struct):
    odometer: Optional[HMApiValue[DataWithUnit[float]]] = None


@register_brand(rate_limit=24 * 60 * 60)
class KiaInfo(HMApiResponse):
    diagnostics: Optional[KiaDiagnostics] = None


class MergedKiaDiagnostics(msgspec.Struct):
    odometer: list[HMApiValue[DataWithUnit[float]]] = []

    @classmethod
    def from_initial(cls, initial: Optional[KiaDiagnostics]) -> Self:
        ret = cls()
        if initial is None or initial.odometer is None:
            ret.odometer = []
        else:
            ret.odometer = [initial.odometer]
        return ret

    def merge(self, other: Optional[KiaDiagnostics]):
        if other is not None and is_new_value(self.odometer, other.odometer):
            self.odometer.append(cast(HMApiValue[DataWithUnit[float]], other.odometer))


@register_merged
class MergedKiaInfo(msgspec.Struct):
    diagnostics: MergedKiaDiagnostics

    @classmethod
    def new(cls) -> Self:
        return cls(MergedKiaDiagnostics())

    @classmethod
    def from_initial(cls, initial: KiaInfo) -> Self:
        return cls(MergedKiaDiagnostics.from_initial(initial.diagnostics))

    def merge(self, other: KiaInfo):
        self.diagnostics.merge(other.diagnostics)

