from dataclasses import dataclass


@dataclass(frozen=True, slots=True, eq=False)
class Vehicle:
    vin: str
    brand: str
    rate_limit: int = 35
    clearance_status: str | None = None

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, Vehicle):
            return self.vin == value.vin
        return False

    def __hash__(self) -> int:
        return hash((self.vin, self.brand, self.rate_limit))
