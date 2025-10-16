import asyncio

import msgspec

from core.s3.async_s3 import AsyncS3
from ingestion.mobilisights.schema import CarState, MergedCarState
from transform.compressor.compressor import Compressor


class MobilisightCompressor(Compressor):
    def __init__(self, make="stellantis") -> None:
        super().__init__()
        self.make = make
        self._s3 = AsyncS3()

    @property
    def brand_prefix(self) -> str:
        return self.make

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        data = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file, type=CarState)
            data.append(decoded)
        merged_data = MergedCarState.from_list(data)
        return msgspec.json.encode(merged_data)

    @classmethod
    async def compress(cls, make: str):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()

