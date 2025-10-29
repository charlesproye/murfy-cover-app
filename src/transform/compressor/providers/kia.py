import asyncio

import msgspec

from core.s3.async_s3 import AsyncS3
from transform.compressor.compressor import Compressor


class KiaCompressor(Compressor):
    def __init__(self, make="kia") -> None:
        super().__init__()
        self.make = make
        self._s3 = AsyncS3()

    @property
    def brand_prefix(self) -> str:
        return self.make

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        all_items = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            if isinstance(decoded, list):
                all_items.extend(decoded)
            else:
                all_items.append(decoded)
        return msgspec.json.encode(all_items)

    @classmethod
    async def compress(cls, make):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()

