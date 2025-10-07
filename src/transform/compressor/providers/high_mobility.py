import asyncio

import msgspec

from core.s3.async_s3 import AsyncS3
from ingestion.high_mobility.schema.brands.ford import FordInfo, MergedFordInfo
from ingestion.high_mobility.schema.brands.kia import KiaInfo, MergedKiaInfo
from ingestion.high_mobility.schema.brands.mercedes_benz import (
    MercedesBenzInfo,
    MergedMercedesBenzInfo,
)
from ingestion.high_mobility.schema.brands.renault import MergedRenaultInfo, RenaultInfo
from ingestion.high_mobility.schema.brands.volvo_cars import MergedVolvoInfo, VolvoInfo
from transform.compressor.compressor import Compressor


class HighMobilityCompressor(Compressor):
    def __init__(self, make) -> None:
        self.make = make
        self._s3 = AsyncS3()
        self._s3_dev = AsyncS3(env="dev")
        self.schemas = {
            "kia": (KiaInfo, MergedKiaInfo),
            "ford": (FordInfo, MergedFordInfo),
            "mercedes-benz": (MercedesBenzInfo, MergedMercedesBenzInfo),
            "renault": (RenaultInfo, MergedRenaultInfo),
            "volvo-cars": (VolvoInfo, MergedVolvoInfo),
        }

    @property
    def brand_prefix(self) -> str:
        return self.make

    def merged_is_empty(self, merged_obj) -> bool:
        """
        Returns True if all array fields in a Merged* object are empty.
        Recursively checks nested msgspec.Structs.
        """
        # Iterate over all declared fields
        for field in type(merged_obj).__annotations__:
            value = getattr(merged_obj, field, None)

            if value is None:
                continue

            # If value is a msgspec.Struct, recurse
            if isinstance(value, msgspec.Struct):
                if not self.merged_is_empty(value):
                    return False
            # If value is a list/array, check if empty
            elif isinstance(value, list):
                if len(value) > 0:
                    return False
            else:
                # Other types can be ignored or treated as empty
                continue

        return True

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes | None:
        brand_info_cls, merged_cls = self.schemas[self.make]

        merged = merged_cls.new()

        for file in new_files.values():
            decoded: brand_info_cls = msgspec.json.decode(file, type=brand_info_cls)

            merged.merge(decoded)

        # Cope with empty data
        if self.merged_is_empty(merged):
            return None
        return msgspec.json.encode(merged)

    @classmethod
    async def compress(cls, make: str):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()

