from typing import Type

from ingestion.high_mobility.schema import (
    BmwInfo,
    KiaInfo,
    RenaultInfo,
)
from ingestion.high_mobility.schema.merged import (
    MergedBmwInfo,
    MergedKiaInfo,
    MergedMercedesBenzInfo,
    MergedRenaultInfo,
)


class Brand:
    info_class: Type
    merged_info_class: Type
    rate_limit: int

    def __init__(
        self, info_class: Type, merged_info_class: Type, rate_limit: int
    ) -> None:
        self.info_class = info_class
        self.merged_info_class = merged_info_class
        self.rate_limit = rate_limit


# Correspondance entre les noms de marques normalisés et les classes des données
brands: dict[str, Brand] = {
    "kia": Brand(KiaInfo, MergedKiaInfo, 24 * 60 * 60),
    "renault": Brand(RenaultInfo, MergedRenaultInfo, 36),
    "mercedes-benz": Brand(RenaultInfo, MergedMercedesBenzInfo, 36),
    "bmw": Brand(BmwInfo, MergedBmwInfo, 36),
}

