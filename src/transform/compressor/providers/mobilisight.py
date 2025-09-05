import asyncio
import msgspec
from transform.compressor.compressor import Compressor
from core.s3.async_s3 import AsyncS3
import io

class MobilisightCompressor(Compressor):
    def __init__(self, make='stellantis') -> None:
        super().__init__()
        self.make = make
        self._s3 = AsyncS3()

    @property
    def brand_prefix(self) -> str:
        return self.make

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        buf = io.BytesIO()
        encoder = msgspec.json.Encoder()

        buf.write(b"[")  # début JSON array
        first = True
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            if isinstance(decoded, str):
                decoded = msgspec.json.decode(decoded.encode())
            if not first:
                buf.write(b",")
            buf.write(encoder.encode(decoded))
            first = False
        buf.write(b"]")  # fin JSON array

        return buf.getvalue()

    @classmethod
    async def compress(cls, make: str):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()
