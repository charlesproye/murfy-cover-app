import asyncio

import msgspec
from core.s3.async_s3 import AsyncS3
from datetime import datetime


class Compressor:
    """Compressor class that could be extended or modified easily to be used for all car brand compression"""

    def __init__(self, brand_prefix: str, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()
        self.brand_prefix = brand_prefix

    async def run(self):
        vins_folders, _ = await self._s3.list_content(f"response/{self.brand_prefix}/")
        print(f"{vins_folders = }")
        await asyncio.gather(
            *(self.compress_temp_vin_data(vin_path) for vin_path in vins_folders)
        )

    async def compress_temp_vin_data(self, vin_folder_path: str):
        new_files = await self._s3.download_folder(f"{vin_folder_path}temp/")
        data = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            json = msgspec.json.decode(decoded.encode())
            data.append(json)
        encoded = msgspec.json.encode({"data": data})
        await self._s3.upload_file(
            path=f"{vin_folder_path}{self.filename()}", file=encoded
        )
        await self._s3.delete_folder(f"{vin_folder_path}temp/")

    def filename(self):
        return f"{datetime.now().strftime('%Y-%m-%d')}.json"

