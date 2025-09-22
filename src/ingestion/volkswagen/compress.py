import asyncio

import msgspec
from core.compressor import Compressor


class VolkswagenCompressor(Compressor):
    @property
    def brand_prefix(self) -> str:
        return "volkswagen"

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        data = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            data.append(decoded)
        return msgspec.json.encode({"data": data})


async def compress():
    compressor = VolkswagenCompressor()
    await compressor.run()


if __name__ == "__main__":
    asyncio.run(compress())

