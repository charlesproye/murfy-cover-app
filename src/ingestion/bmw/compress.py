import asyncio

import msgspec
from src.core.compressor import Compressor


class BMWCompressor(Compressor):
    @property
    def brand_prefix(self) -> str:
        return "bmw"

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        data = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            if isinstance(decoded,str):
                decoded = msgspec.json.decode(decoded.encode())
            data.append(decoded)
        return msgspec.json.encode({"data": data})


async def compress():
    asyncio.get_event_loop().set_debug(False)
    compressor = BMWCompressor()
    await compressor.run()


if __name__ == "__main__":
    asyncio.run(compress())

