import asyncio

import msgspec
from src.core.s3.async_s3 import AsyncS3
from datetime import datetime
from abc import ABC, abstractmethod

class Compressor(ABC):
    """Compressor class that could be extended or modified easily to be used for all car brand compression"""

    def __init__(self, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()

    @property
    @abstractmethod
    def brand_prefix(self)->str:
        pass

    async def run(self):
        vins_folders, _ = await self._s3.list_content(f"response/{self.brand_prefix}/")
        print(f"{vins_folders = }")
        await asyncio.gather(
            *(self.compress_temp_vin_data(vin_path) for vin_path in vins_folders)
        )

    async def compress_temp_vin_data(self, vin_folder_path: str):
        new_files = await self._s3.download_folder(f"{vin_folder_path}temp/")
        if len(new_files) == 0:
            return
        encoded_data = self.temp_data_to_daily_file(new_files)
        await self._s3.upload_file(
            path=f"{vin_folder_path}{self.filename()}", file=encoded_data
        )
        await self._s3.delete_folder(f"{vin_folder_path}temp/")
    
    @abstractmethod
    def temp_data_to_daily_file(self, new_files:dict[str,bytes])->bytes:
        pass


    def filename(self):
        return f"{datetime.now().strftime('%Y-%m-%d')}.json"

