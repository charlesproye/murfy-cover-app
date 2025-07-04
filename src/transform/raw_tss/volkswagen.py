import asyncio
from datetime import datetime


import msgspec
from src.core.response_to_raw import ResponseToRaw


class VWResponseToRaw(ResponseToRaw):
    async def _get_files_to_add(self, last_date: datetime) -> list[dict]:
        path_to_download: list[str] = await self._paths_to_download(last_date)
        new_data = await self._s3.get_files(path_to_download)
        json_data = []
        for data in new_data.values():
            decoded = msgspec.json.decode(data)
            json_data.append(decoded)
        return json_data


if __name__ == "__main__":
    RESPONSE_TO_RAW = VWResponseToRaw("volkswagen")
    asyncio.run(RESPONSE_TO_RAW.convert())

