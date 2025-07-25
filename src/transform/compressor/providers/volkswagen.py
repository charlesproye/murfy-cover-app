import asyncio

import msgspec
from src.core.compressor import Compressor


class VolkswagenCompressor(Compressor):
    def __init__(self, make='volkswagen') -> None:
        super().__init__()
        self.make = make
        self._s3 = AsyncS3()

    @property
    def brand_prefix(self) -> str:
        return self.make

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        data = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            json = msgspec.json.decode(decoded.encode())
            data.append(json)
        return msgspec.json.encode({"data": data})


    @classmethod
    async def compress(cls, make):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()

