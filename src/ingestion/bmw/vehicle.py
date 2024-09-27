from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True, eq=False)
class Vehicle:
    vin: str
    brand: str
    clearance_status: str
    licence_plate: Optional[str] = None
    note: Optional[str] = None
    contract_end_date: Optional[str] = None
    added_to_fleet: Optional[str] = None
    rate_limit: int = 0

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, Vehicle):
            return self.vin == value.vin
        return False

    def __hash__(self) -> int:
        return hash((self.vin, self.brand, self.rate_limit))
